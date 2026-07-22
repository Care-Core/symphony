defmodule SymphonyElixir.ProcessTree do
  @moduledoc false

  @process_group_exec ~S|POSIX::setpgid(0, 0) == 0 or die "setpgid failed: $!\n"; exec @ARGV; die "exec failed: $!\n";|
  @probe_exec ~S|my $target = shift; exit 0 if kill 0, $target; exit 1 if $!{ESRCH}; my $errno = 0 + $!; print STDERR "process liveness probe failed: errno=$errno ($!)\n"; exit 2;|
  @poll_interval_ms 25

  @type command_result :: %{status: non_neg_integer(), output: binary()}

  @doc false
  @spec validate_launcher(keyword()) :: :ok | {:error, term()}
  def validate_launcher(opts \\ []) do
    case process_group_launcher(opts) do
      {:ok, _executable, _prefix_args} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec open_port(Path.t(), [String.t()], keyword()) :: {:ok, port()} | {:error, term()}
  def open_port(executable, args, opts \\ []) when is_binary(executable) and is_list(args) do
    with {:ok, launcher, prefix_args} <- process_group_launcher(opts),
         true <- File.regular?(executable) or {:error, {:command_not_found, executable}} do
      port_options =
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args:
            Enum.map(
              prefix_args ++ [executable | args],
              &String.to_charlist/1
            )
        ]
        |> maybe_put_option(:cd, Keyword.get(opts, :cd), &String.to_charlist/1)
        |> maybe_put_option(:line, Keyword.get(opts, :line), & &1)

      {:ok,
       Port.open(
         {:spawn_executable, String.to_charlist(launcher)},
         port_options
       )}
    end
  rescue
    error -> {:error, {:process_tree_port_open_failed, error}}
  end

  @spec run(Path.t(), [String.t()], keyword()) ::
          {:ok, command_result()} | {:error, {:timeout, pos_integer(), binary()} | term()}
  def run(executable, args, opts \\ []) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    max_output_bytes = Keyword.get(opts, :max_output_bytes, 64_000)

    with {:ok, port} <- open_port(executable, args, opts) do
      deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
      collect(port, deadline_ms, timeout_ms, max_output_bytes, "")
    end
  end

  @spec terminate_port(port(), pos_integer()) :: :ok | {:error, term()}
  def terminate_port(port, timeout_ms) when is_port(port) and is_integer(timeout_ms) and timeout_ms > 0 do
    cleanup_result =
      case port_os_pid(port) do
        {:ok, os_pid} -> terminate_os_process_tree(os_pid, timeout_ms)
        :error -> :ok
      end

    close_port(port)
    cleanup_result
  end

  @spec terminate_os_process_tree(pos_integer(), pos_integer()) :: :ok | {:error, term()}
  def terminate_os_process_tree(os_pid, timeout_ms)
      when is_integer(os_pid) and os_pid > 0 and is_integer(timeout_ms) and timeout_ms > 0 do
    terminate_os_process_trees([os_pid], timeout_ms)
  end

  @spec terminate_os_process_trees([pos_integer()], pos_integer()) :: :ok | {:error, term()}
  def terminate_os_process_trees(os_pids, timeout_ms)
      when is_list(os_pids) and is_integer(timeout_ms) and timeout_ms > 0 do
    terminate_os_process_trees(os_pids, timeout_ms, [])
  end

  @doc false
  @spec terminate_os_process_trees_for_test([pos_integer()], pos_integer(), keyword()) ::
          :ok | {:error, term()}
  def terminate_os_process_trees_for_test(os_pids, timeout_ms, opts)
      when is_list(os_pids) and is_integer(timeout_ms) and timeout_ms > 0 and is_list(opts) do
    terminate_os_process_trees(os_pids, timeout_ms, opts)
  end

  defp terminate_os_process_trees(os_pids, timeout_ms, opts) do
    descendant_fetcher = Keyword.get(opts, :descendant_fetcher, &descendant_pids/1)
    probe = Keyword.get(opts, :probe, &probe_target/1)
    signaler = Keyword.get(opts, :signaler, &run_kill/2)
    root_pids = os_pids |> Enum.filter(&(is_integer(&1) and &1 > 0)) |> Enum.uniq()
    tracked_pids = root_pids |> Enum.flat_map(&[&1 | descendant_fetcher.(&1)]) |> Enum.uniq()
    started_at_ms = System.monotonic_time(:millisecond)
    terminate_deadline_ms = started_at_ms + max(1, div(timeout_ms, 2))
    kill_deadline_ms = started_at_ms + timeout_ms

    signal_process_groups(root_pids, "TERM", signaler)
    signal_pids(Enum.reverse(tracked_pids), "TERM", signaler)

    unless wait_for_exit(tracked_pids, root_pids, terminate_deadline_ms, probe) == :ok do
      signal_process_groups(root_pids, "KILL", signaler)
      signal_pids(Enum.reverse(tracked_pids), "KILL", signaler)
      wait_for_exit(tracked_pids, root_pids, kill_deadline_ms, probe)
    end

    verify_processes_absent(tracked_pids, root_pids, probe)
  end

  defp collect(port, deadline_ms, timeout_ms, max_output_bytes, output) do
    remaining_ms = max(0, deadline_ms - System.monotonic_time(:millisecond))

    receive do
      {^port, {:data, data}} ->
        collect(
          port,
          deadline_ms,
          timeout_ms,
          max_output_bytes,
          append_output(output, data, max_output_bytes)
        )

      {^port, {:exit_status, status}} ->
        {:ok, %{status: status, output: output}}
    after
      remaining_ms ->
        terminate_port(port, min(timeout_ms, 2_000))
        {:error, {:timeout, timeout_ms, output}}
    end
  end

  defp append_output(output, {:eol, data}, max_output_bytes),
    do: append_output(output, [data, "\n"], max_output_bytes)

  defp append_output(output, {:noeol, data}, max_output_bytes),
    do: append_output(output, data, max_output_bytes)

  defp append_output(output, data, max_output_bytes) do
    remaining = max(0, max_output_bytes - byte_size(output))

    if remaining == 0 do
      output
    else
      data = IO.iodata_to_binary(data)
      output <> binary_part(data, 0, min(remaining, byte_size(data)))
    end
  end

  defp find_executable(executable) when is_binary(executable) do
    case System.find_executable(executable) do
      nil -> {:error, {:command_not_found, executable}}
      path -> {:ok, path}
    end
  end

  defp process_group_launcher(opts) do
    perl = Keyword.get(opts, :perl, "perl")
    setsid = Keyword.get(opts, :setsid, "setsid")

    case find_executable(perl) do
      {:ok, executable} ->
        {:ok, executable, ["-MPOSIX", "-e", @process_group_exec]}

      {:error, _reason} ->
        case find_executable(setsid) do
          {:ok, executable} -> {:ok, executable, []}
          {:error, _reason} -> {:error, {:process_group_launcher_not_found, [perl, setsid]}}
        end
    end
  end

  defp maybe_put_option(options, _key, nil, _transform), do: options

  defp maybe_put_option(options, key, value, transform) do
    Keyword.put(options, key, transform.(value))
  end

  defp port_os_pid(port) do
    case :erlang.port_info(port, :os_pid) do
      {:os_pid, os_pid} when is_integer(os_pid) and os_pid > 0 -> {:ok, os_pid}
      _ -> :error
    end
  end

  defp descendant_pids(root_pid) do
    child_pid_map()
    |> descendants_from(root_pid, [])
  end

  defp child_pid_map do
    case System.find_executable("ps") do
      nil ->
        %{}

      ps ->
        case System.cmd(ps, ["-axo", "pid=,ppid="], stderr_to_stdout: true) do
          {output, 0} -> parse_process_table(output)
          _ -> %{}
        end
    end
  rescue
    _ -> %{}
  end

  defp parse_process_table(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reduce(%{}, fn line, acc ->
      case line |> String.split(~r/\s+/, trim: true) |> Enum.map(&Integer.parse/1) do
        [{pid, ""}, {parent_pid, ""}] -> Map.update(acc, parent_pid, [pid], &[pid | &1])
        _ -> acc
      end
    end)
  end

  defp descendants_from(child_map, parent_pid, acc) do
    Enum.reduce(Map.get(child_map, parent_pid, []), acc, fn child_pid, descendants ->
      descendants_from(child_map, child_pid, [child_pid | descendants])
    end)
  end

  defp wait_for_exit(tracked_pids, process_group_ids, deadline_ms, probe) do
    case verify_processes_absent(tracked_pids, process_group_ids, probe) do
      :ok ->
        :ok

      {:error, {:process_tree_cleanup_unconfirmed, _details}} ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          :timeout
        else
          Process.sleep(@poll_interval_ms)
          wait_for_exit(tracked_pids, process_group_ids, deadline_ms, probe)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp verify_processes_absent(tracked_pids, process_group_ids, probe) do
    with {:ok, alive_pids} <- alive_targets(tracked_pids, probe),
         {:ok, alive_process_groups} <- alive_targets(Enum.map(process_group_ids, &(-&1)), probe) do
      case {alive_pids, alive_process_groups} do
        {[], []} ->
          :ok

        _ ->
          {:error,
           {:process_tree_cleanup_unconfirmed,
            %{
              alive_pids: alive_pids,
              alive_process_groups: Enum.map(alive_process_groups, &abs/1)
            }}}
      end
    end
  end

  defp alive_targets(targets, probe) do
    Enum.reduce_while(targets, {:ok, []}, fn target, {:ok, alive} ->
      case probe.(target) do
        :alive -> {:cont, {:ok, [target | alive]}}
        :absent -> {:cont, {:ok, alive}}
        {:error, reason} -> {:halt, {:error, {:process_tree_probe_failed, target, reason}}}
      end
    end)
    |> case do
      {:ok, alive} -> {:ok, Enum.reverse(alive)}
      {:error, _reason} = error -> error
    end
  end

  defp signal_process_groups(process_group_ids, signal, signaler) do
    Enum.each(process_group_ids, &signaler.(signal, -&1))
  end

  defp signal_pids(pids, signal, signaler) do
    Enum.each(pids, &signaler.(signal, &1))
  end

  defp probe_target(target) do
    case System.find_executable("perl") do
      nil ->
        {:error, :probe_unavailable}

      perl ->
        case System.cmd(perl, ["-MErrno=ESRCH", "-e", @probe_exec, "--", Integer.to_string(target)], stderr_to_stdout: true) do
          {_output, 0} -> :alive
          {_output, 1} -> :absent
          {output, status} -> {:error, {:probe_failed, status, output}}
        end
    end
  rescue
    error -> {:error, {:probe_exception, error}}
  end

  defp run_kill(signal, target) do
    case System.find_executable("kill") do
      nil -> :ok
      kill -> System.cmd(kill, ["-#{signal}", Integer.to_string(target)], stderr_to_stdout: true)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp close_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        :ok

      _ ->
        try do
          Port.close(port)
        rescue
          ArgumentError -> :ok
        end
    end
  end
end
