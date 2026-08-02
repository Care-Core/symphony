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
  @file_type_mask 0o170000
  @fifo_type 0o010000

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
    @spec read_fd_for_test(integer(), (integer() -> term()), non_neg_integer()) :: term()
    def read_fd_for_test(fd, reader, timeout_ms),
      do: read_fd_with_timeout(fd, reader, timeout_ms)

    @doc false
    @spec read_bounded_for_test((term(), pos_integer() -> term())) :: term()
    def read_bounded_for_test(reader), do: read_bounded(:test_file_ref, <<>>, reader)
  end

  @impl true
  def init(opts) do
    source = Keyword.get(opts, :source, :environment)

    case load(source) do
      {:ok, token} -> {:ok, token}
      {:error, reason} -> {:stop, {:invalid_control_token_fd, reason}}
    end
  end

  @impl true
  def handle_call(:fetch, _from, nil), do: {:reply, {:error, :control_token_not_configured}, nil}

  def handle_call(:fetch, _from, token) when is_binary(token),
    do: {:reply, {:ok, token}, token}

  if Mix.env() == :test do
    @impl true
    def handle_call({:replace_for_test, token}, _from, _state)
        when is_nil(token) or is_binary(token) do
      {:reply, :ok, token}
    end
  end

  defp load(:environment) do
    case System.get_env(@fd_env) do
      nil -> {:ok, nil}
      value -> value |> parse_fd() |> read_configured_fd()
    end
  end

  defp load({:token, token}) when is_binary(token), do: validate_token(token)

  defp parse_fd(value) do
    case Integer.parse(value) do
      {fd, ""} when fd >= 3 -> {:ok, fd}
      _ -> {:error, :invalid_fd_number}
    end
  end

  defp read_configured_fd({:ok, fd}), do: read_fd_with_timeout(fd)
  defp read_configured_fd({:error, reason}), do: {:error, reason}

  defp read_fd_with_timeout(fd),
    do: read_fd_with_timeout(fd, &adopt_read_and_close/1, @read_timeout_ms)

  defp read_fd_with_timeout(fd, reader_function, timeout_ms) do
    parent = self()
    result_ref = make_ref()

    {reader, monitor} =
      spawn_monitor(fn ->
        result = reader_function.(fd)
        send(parent, {result_ref, result})
      end)

    receive do
      {^result_ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^reader, _reason} ->
        close_unadopted_fd(fd)
        {:error, :read_failed}
    after
      timeout_ms ->
        Process.exit(reader, :kill)
        await_reader_down(monitor, reader)
        close_unadopted_fd(fd)
        {:error, :read_timeout}
    end
  end

  defp adopt_read_and_close(fd) do
    case :prim_file.file_desc_to_ref(fd, [:read, :binary]) do
      {:ok, file_ref} ->
        try do
          with :ok <- require_pipe(file_ref),
               {:ok, token} <- read_bounded(file_ref, <<>>) do
            validate_token(token)
          end
        after
          :ok = :prim_file.close(file_ref)
        end

      {:error, _reason} ->
        {:error, :invalid_fd}
    end
  end

  defp require_pipe(file_ref) do
    {:ok, info} = :prim_file.read_handle_info(file_ref, time: :posix)
    mode = file_info(info, :mode)

    if band(mode, @file_type_mask) == @fifo_type,
      do: :ok,
      else: {:error, :not_a_pipe}
  end

  defp read_bounded(file_ref, data), do: read_bounded(file_ref, data, &:prim_file.read/2)

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
    if String.contains?(token, [<<0>>, "\r", "\n"]),
      do: {:error, :invalid_token_bytes},
      else: {:ok, token}
  end

  defp await_reader_down(monitor, reader) do
    receive do
      {:DOWN, ^monitor, :process, ^reader, _reason} -> :ok
    end
  end

  defp close_unadopted_fd(fd) do
    with {:ok, file_ref} <- :prim_file.file_desc_to_ref(fd, [:read, :binary]) do
      :prim_file.close(file_ref)
    end

    :ok
  end
end
