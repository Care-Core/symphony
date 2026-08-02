defmodule SymphonyElixir.ControlToken do
  @moduledoc """
  Owns the optional loopback control token without retaining it in the OS environment.

  Production startup accepts only an inherited anonymous pipe descriptor named by
  `SYMPHONY_CONTROL_TOKEN_FD`. The descriptor is adopted once, read with a bounded
  size and timeout, and closed before the rest of the application starts.
  """

  use GenServer

  import Bitwise

  require Record

  Record.defrecordp(:file_info, Record.extract(:file_info, from_lib: "kernel/include/file.hrl"))

  @fd_env "SYMPHONY_CONTROL_TOKEN_FD"
  @legacy_env "SYMPHONY_CONTROL_TOKEN"
  @max_token_bytes 4_096
  @read_timeout_ms 2_000
  @reader_shutdown_timeout_ms 250
  @file_type_mask 0o170000
  @fifo_type 0o010000
  @persistent_key {__MODULE__, :token}

  @type fetch_result :: {:ok, String.t()} | {:error, :control_token_not_configured}

  @doc "Returns the names that must never reach an agent child environment."
  @spec secret_environment_names() :: [String.t()]
  def secret_environment_names, do: [@legacy_env, @fd_env]

  @doc "Starts the owner-side token store."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Consumes the inherited descriptor once and returns a restart-stable child specification."
  @spec child_spec_from_environment(keyword()) ::
          {:ok, Supervisor.child_spec()} | {:error, atom()}
  def child_spec_from_environment(opts \\ []) do
    id = Keyword.get(opts, :id, __MODULE__)
    child_opts = Keyword.delete(opts, :id)

    case load(:environment) do
      {:ok, token} ->
        store_token(token)

        {:ok,
         Supervisor.child_spec(
           {__MODULE__, Keyword.put(child_opts, :source, :prepared)},
           id: id
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Returns the configured owner control token without consulting the OS environment."
  @spec fetch(GenServer.server()) :: fetch_result()
  def fetch(server \\ __MODULE__), do: GenServer.call(server, :fetch)

  if Mix.env() == :test do
    @doc false
    @spec replace_for_test(String.t() | nil) :: :ok
    def replace_for_test(token) when is_nil(token) or is_binary(token) do
      GenServer.call(__MODULE__, {:replace_for_test, token})
    end

    @doc false
    @spec read_fd_for_test(integer(), (term() -> term()), non_neg_integer()) :: term()
    def read_fd_for_test(fd, reader, timeout_ms),
      do: read_fd_with_timeout(fd, reader, timeout_ms)

    @doc false
    @spec read_bounded_for_test((term(), pos_integer() -> term())) :: term()
    def read_bounded_for_test(reader), do: read_bounded(:test_file_ref, <<>>, reader)

    @doc false
    @spec normalize_private_reader_for_test(term()) :: {:ok, pid()} | {:error, :invalid_fd}
    def normalize_private_reader_for_test(result), do: normalize_private_reader(result)

    @doc false
    @spec await_process_down_for_test(pid(), non_neg_integer()) :: :ok | :timeout
    def await_process_down_for_test(process, timeout_ms) do
      process
      |> Process.monitor()
      |> await_process_down(process, timeout_ms)
    end
  end

  @impl true
  def init(opts) do
    source = Keyword.get(opts, :source, :environment)

    case source do
      :prepared ->
        {:ok, :ready}

      _ ->
        case load(source) do
          {:ok, token} ->
            store_token(token)
            {:ok, :ready}

          {:error, reason} ->
            {:stop, {:invalid_control_token_fd, reason}}
        end
    end
  end

  @impl true
  def handle_call(:fetch, _from, state), do: {:reply, stored_token_result(), state}

  if Mix.env() == :test do
    @impl true
    def handle_call({:replace_for_test, token}, _from, state)
        when is_nil(token) or is_binary(token) do
      store_token(token)
      {:reply, :ok, state}
    end
  end

  defp load(:environment) do
    descriptor = System.get_env(@fd_env)
    System.delete_env(@fd_env)
    System.delete_env(@legacy_env)

    case descriptor do
      nil -> {:ok, stored_token()}
      value -> value |> parse_fd() |> read_configured_fd()
    end
  end

  defp load({:token, token}) when is_binary(token), do: validate_token(token)

  defp store_token(token) when is_nil(token) or is_binary(token) do
    :persistent_term.put(@persistent_key, token)
  end

  defp stored_token, do: :persistent_term.get(@persistent_key, nil)

  defp stored_token_result do
    case stored_token() do
      nil -> {:error, :control_token_not_configured}
      token -> {:ok, token}
    end
  end

  defp parse_fd(value) do
    case Integer.parse(value) do
      {fd, ""} when fd >= 3 -> {:ok, fd}
      _ -> {:error, :invalid_fd_number}
    end
  end

  defp read_configured_fd({:ok, fd}), do: read_fd_with_timeout(fd)
  defp read_configured_fd({:error, reason}), do: {:error, reason}

  defp read_fd_with_timeout(fd), do: read_fd_with_timeout(fd, &read_and_validate/1, @read_timeout_ms)

  defp read_fd_with_timeout(fd, reader_function, timeout_ms) do
    case :prim_file.file_desc_to_ref(fd, [:read, :binary]) do
      {:ok, source_ref} ->
        try do
          with :ok <- require_pipe(source_ref),
               {:ok, io_server} <- open_private_reader(fd) do
            read_io_server_with_timeout(io_server, reader_function, timeout_ms)
          end
        after
          :ok = :prim_file.close(source_ref)
        end

      {:error, _reason} ->
        {:error, :invalid_fd}
    end
  end

  defp open_private_reader(fd) do
    "/dev/fd/#{fd}"
    |> String.to_charlist()
    |> :file.open([:read, :binary])
    |> normalize_private_reader()
  end

  defp normalize_private_reader({:ok, io_server}) when is_pid(io_server), do: {:ok, io_server}

  defp normalize_private_reader({:ok, io_device}) do
    :file.close(io_device)
    {:error, :invalid_fd}
  end

  defp normalize_private_reader({:error, _reason}), do: {:error, :invalid_fd}

  defp read_io_server_with_timeout(io_server, reader_function, timeout_ms) do
    parent = self()
    result_ref = make_ref()

    {reader, monitor} =
      spawn_monitor(fn ->
        result = reader_function.(io_server)
        send(parent, {result_ref, result})
      end)

    receive do
      {^result_ref, result} ->
        Process.demonitor(monitor, [:flush])
        :ok = :file.close(io_server)
        result

      {:DOWN, ^monitor, :process, ^reader, _reason} ->
        terminate_io_server(io_server)
        {:error, :read_failed}
    after
      timeout_ms ->
        terminate_io_server(io_server)
        Process.exit(reader, :kill)
        await_process_down(monitor, reader)
        {:error, :read_timeout}
    end
  end

  defp read_and_validate(file_ref) do
    with {:ok, token} <- read_bounded(file_ref, <<>>) do
      validate_token(token)
    end
  end

  defp require_pipe(file_ref) do
    {:ok, info} = :prim_file.read_handle_info(file_ref, time: :posix)
    mode = file_info(info, :mode)

    if band(mode, @file_type_mask) == @fifo_type,
      do: :ok,
      else: {:error, :not_a_pipe}
  end

  defp read_bounded(io_device, data), do: read_bounded(io_device, data, &:file.read/2)

  defp read_bounded(file_ref, data, reader) when byte_size(data) <= @max_token_bytes do
    read_size = min(1_024, @max_token_bytes + 1 - byte_size(data))

    case reader.(file_ref, read_size) do
      :eof -> {:ok, data}
      {:ok, chunk} -> read_bounded_chunk(file_ref, data, chunk, reader)
      {:error, _reason} -> {:error, :read_failed}
    end
  end

  defp read_bounded_chunk(_file_ref, data, chunk, _reader)
       when byte_size(data) + byte_size(chunk) > @max_token_bytes,
       do: {:error, :token_too_large}

  defp read_bounded_chunk(file_ref, data, chunk, reader),
    do: read_bounded(file_ref, data <> chunk, reader)

  defp validate_token(<<>>), do: {:error, :empty_token}

  defp validate_token(token) when byte_size(token) > @max_token_bytes,
    do: {:error, :token_too_large}

  defp validate_token(token) do
    if token |> :binary.bin_to_list() |> Enum.all?(&(&1 in 0x21..0x7E)),
      do: {:ok, token},
      else: {:error, :invalid_token_bytes}
  end

  defp await_process_down(monitor, process, timeout_ms \\ @reader_shutdown_timeout_ms) do
    receive do
      {:DOWN, ^monitor, :process, ^process, _reason} -> :ok
    after
      timeout_ms ->
        Process.demonitor(monitor, [:flush])
        :timeout
    end
  end

  defp terminate_io_server(io_server) do
    monitor = Process.monitor(io_server)
    Process.exit(io_server, :kill)
    await_process_down(monitor, io_server)
  end
end
