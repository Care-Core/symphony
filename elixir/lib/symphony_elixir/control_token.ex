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
  def child_spec_from_environment(opts \\ []), do: child_spec_from_source(:environment, opts)

  if Mix.env() == :test do
    @doc false
    @spec child_spec_from_token_for_test(String.t(), keyword()) ::
            {:ok, Supervisor.child_spec()} | {:error, atom()}
    def child_spec_from_token_for_test(token, opts \\ []),
      do: child_spec_from_source({:token, token}, opts)
  end

  defp child_spec_from_source(source, opts) do
    id = Keyword.get(opts, :id, __MODULE__)
    child_opts = Keyword.delete(opts, :id)

    case load(source) do
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
    @spec read_fd_for_test(integer(), non_neg_integer()) :: term()
    def read_fd_for_test(fd, timeout_ms), do: read_fd_with_timeout(fd, timeout_ms)

    @doc false
    @spec open_private_port_for_test(integer()) :: {:ok, port()} | {:error, :invalid_fd}
    def open_private_port_for_test(fd), do: open_private_port(fd)

    @doc false
    @spec read_private_port_for_test(port(), non_neg_integer()) :: term()
    def read_private_port_for_test(port, timeout_ms), do: read_port_with_timeout(port, timeout_ms)
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

  defp read_fd_with_timeout(fd), do: read_fd_with_timeout(fd, @read_timeout_ms)

  defp read_fd_with_timeout(fd, timeout_ms) do
    case :prim_file.file_desc_to_ref(fd, [:read, :binary]) do
      {:ok, source_ref} ->
        try do
          with :ok <- require_pipe(source_ref),
               {:ok, port} <- open_private_port(fd) do
            read_port_with_timeout(port, timeout_ms)
          end
        after
          :ok = :prim_file.close(source_ref)
        end

      {:error, _reason} ->
        {:error, :invalid_fd}
    end
  end

  defp open_private_port(fd) do
    port = Port.open({:fd, fd, fd}, [:binary, :in, :eof])
    Process.unlink(port)
    {:ok, port}
  rescue
    _error in [ArgumentError, ErlangError] -> {:error, :invalid_fd}
  end

  defp read_port_with_timeout(port, timeout_ms) do
    monitor = :erlang.monitor(:port, port)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    try do
      receive_port(port, monitor, <<>>, deadline)
    after
      close_private_port(port)
      Process.demonitor(monitor, [:flush])
    end
  end

  defp receive_port(port, monitor, data, deadline) do
    remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, chunk}} when is_binary(chunk) ->
        receive_port_chunk(port, monitor, data, chunk, deadline)

      {^port, :eof} ->
        validate_token(data)

      {:DOWN, ^monitor, :port, ^port, _reason} ->
        {:error, :read_failed}
    after
      remaining_ms -> {:error, :read_timeout}
    end
  end

  defp receive_port_chunk(_port, _monitor, data, chunk, _deadline)
       when byte_size(data) + byte_size(chunk) > @max_token_bytes,
       do: {:error, :token_too_large}

  defp receive_port_chunk(port, monitor, data, chunk, deadline),
    do: receive_port(port, monitor, data <> chunk, deadline)

  defp close_private_port(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp require_pipe(file_ref) do
    {:ok, info} = :prim_file.read_handle_info(file_ref, time: :posix)
    mode = file_info(info, :mode)

    if band(mode, @file_type_mask) == @fifo_type,
      do: :ok,
      else: {:error, :not_a_pipe}
  end

  defp validate_token(<<>>), do: {:error, :empty_token}

  defp validate_token(token) when byte_size(token) > @max_token_bytes,
    do: {:error, :token_too_large}

  defp validate_token(token) do
    if token |> :binary.bin_to_list() |> Enum.all?(&(&1 in 0x21..0x7E)),
      do: {:ok, token},
      else: {:error, :invalid_token_bytes}
  end
end
