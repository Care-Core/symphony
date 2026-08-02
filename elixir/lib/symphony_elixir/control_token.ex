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
    @spec read_adopted_fd_for_test(
            integer(),
            term(),
            non_neg_integer(),
            (term(), integer() -> term()),
            (integer() -> term())
          ) ::
            term()
    def read_adopted_fd_for_test(fd, source_ref, timeout_ms, inspector, opener),
      do: read_adopted_fd(fd, source_ref, timeout_ms, inspector, opener)

    @doc false
    @spec open_private_port_for_test(integer()) :: {:ok, port()} | {:error, :invalid_fd}
    def open_private_port_for_test(fd), do: open_private_port(fd)

    @doc false
    @spec read_private_port_for_test(port(), non_neg_integer()) :: term()
    def read_private_port_for_test(port, timeout_ms), do: read_port_with_timeout(port, timeout_ms)

    @doc false
    @spec descriptor_link_for_test(:os.type(), integer()) :: term()
    def descriptor_link_for_test(os_type, fd), do: descriptor_link(os_type, fd)

    @doc false
    @spec require_anonymous_pipe_for_test(term(), integer()) ::
            :ok | {:error, :invalid_fd | :not_a_pipe | :not_anonymous_pipe | :unsupported_platform}
    def require_anonymous_pipe_for_test(source_ref, fd),
      do: require_anonymous_pipe(source_ref, fd)

    @doc false
    @spec validate_pipe_identity_for_test(:os.type(), term(), non_neg_integer()) ::
            :ok | {:error, :not_anonymous_pipe | :unsupported_platform}
    def validate_pipe_identity_for_test(os_type, link_result, device),
      do: validate_pipe_identity(os_type, link_result, device)
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
        read_adopted_fd(
          fd,
          source_ref,
          timeout_ms,
          &require_anonymous_pipe/2,
          &open_private_port/1
        )

      {:error, _reason} ->
        {:error, :invalid_fd}
    end
  end

  defp read_adopted_fd(fd, source_ref, timeout_ms, inspector, opener) do
    with :ok <- inspector.(source_ref, fd),
         {:ok, port} <- opener.(fd) do
      read_port_with_timeout(port, timeout_ms)
    end
  after
    :prim_file.close(source_ref)
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

  defp require_anonymous_pipe(source_ref, fd) do
    case :prim_file.read_handle_info(source_ref, time: :posix) do
      {:ok, info} when band(file_info(info, :mode), @file_type_mask) == @fifo_type ->
        os_type = :os.type()

        validate_pipe_identity(
          os_type,
          descriptor_link(os_type, fd),
          file_info(info, :major_device)
        )

      {:ok, _info} ->
        {:error, :not_a_pipe}

      {:error, _reason} ->
        {:error, :invalid_fd}
    end
  end

  defp descriptor_link({:unix, :linux}, fd), do: File.read_link("/proc/self/fd/#{fd}")
  defp descriptor_link(_os_type, _fd), do: :not_required

  defp validate_pipe_identity({:unix, :linux}, {:ok, target}, _device) do
    if Regex.match?(~r/^pipe:\[\d+\]$/, target), do: :ok, else: {:error, :not_anonymous_pipe}
  end

  defp validate_pipe_identity({:unix, :linux}, _link_result, _device),
    do: {:error, :not_anonymous_pipe}

  defp validate_pipe_identity({:unix, :darwin}, :not_required, 0), do: :ok

  defp validate_pipe_identity({:unix, :darwin}, :not_required, _device),
    do: {:error, :not_anonymous_pipe}

  defp validate_pipe_identity(_os_type, _link_result, _device),
    do: {:error, :unsupported_platform}

  defp validate_token(<<>>), do: {:error, :empty_token}

  defp validate_token(token) when byte_size(token) > @max_token_bytes,
    do: {:error, :token_too_large}

  defp validate_token(token) do
    if token |> :binary.bin_to_list() |> Enum.all?(&(&1 in 0x21..0x7E)),
      do: {:ok, token},
      else: {:error, :invalid_token_bytes}
  end
end
