defmodule SymphonyElixir.SSH do
  @moduledoc false

  alias SymphonyElixir.ProcessTree

  @spec run(String.t(), String.t(), keyword()) :: {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(host, command, opts \\ []) when is_binary(host) and is_binary(command) do
    with {:ok, executable} <- ssh_executable() do
      {:ok, System.cmd(executable, ssh_args(host, command), opts)}
    end
  end

  @spec start_port(String.t(), String.t(), keyword()) :: {:ok, port()} | {:error, term()}
  def start_port(host, command, opts \\ []) when is_binary(host) and is_binary(command) do
    with {:ok, executable} <- ssh_executable() do
      line_bytes = Keyword.get(opts, :line)

      port_opts =
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: Enum.map(ssh_args(host, command), &String.to_charlist/1)
        ]
        |> maybe_put_line_option(line_bytes)

      {:ok, Port.open({:spawn_executable, String.to_charlist(executable)}, port_opts)}
    end
  end

  @spec terminate_remote_process_tree(String.t(), Path.t(), pos_integer()) ::
          :ok | {:error, term()}
  def terminate_remote_process_tree(host, workspace, timeout_ms)
      when is_binary(host) and is_binary(workspace) and is_integer(timeout_ms) and timeout_ms > 0 do
    pid_file = Path.join(workspace, ".symphony-codex-app-server.pid")
    kill_delay_seconds = max(1, div(timeout_ms, 3)) / 1_000
    kill_check_attempts = max(1, div(timeout_ms, 75))

    command = """
    pid_file=#{shell_escape(pid_file)}
    if [ ! -r \"$pid_file\" ]; then
      printf 'remote PID proof file is missing or unreadable\n' >&2
      exit 73
    fi
    if ! pid_proof=$(< \"$pid_file\"); then
      printf 'remote PID proof file is unreadable\n' >&2
      exit 74
    fi
    if [ -z \"$pid_proof\" ]; then
      printf 'remote PID proof file is empty\n' >&2
      exit 75
    fi
    proof_state=${pid_proof%%:*}
    proof_remainder=${pid_proof#*:}
    pid=${proof_remainder%%:*}
    process_identity=${proof_remainder#*:}
    case \"$proof_state\" in
      running) cleanup_confirmed=0 ;;
      stopped) cleanup_confirmed=1 ;;
      *)
        printf 'remote PID proof file has an unsupported format\n' >&2
        exit 76
        ;;
    esac
    case \"$pid\" in
      '')
        printf 'remote PID proof file has an empty PID\n' >&2
        exit 76
        ;;
      *[!0-9]*)
        printf 'remote PID proof file is nonnumeric\n' >&2
        exit 76
        ;;
    esac
    case \"$process_identity\" in
      ''|*[!0-9a-f]*)
        printf 'remote PID proof file has an invalid process identity\n' >&2
        exit 76
        ;;
    esac
    identity_length=${#process_identity}
    if [ \"$identity_length\" -gt 256 ] || [ $((identity_length % 2)) -ne 0 ]; then
      printf 'remote PID proof file has an invalid process identity\n' >&2
      exit 76
    fi

    if ! perl -e '
      my $pid = shift;
      my $max_pid = \"2147483647\";
      exit 1 unless $pid =~ /\\A(?:[2-9]|[1-9][0-9]+)\\z/;
      exit 1 if length($pid) > length($max_pid);
      exit 1 if length($pid) == length($max_pid) && $pid gt $max_pid;
    ' -- \"$pid\"; then
      printf 'remote PID proof file is outside the safe process-group range\n' >&2
      exit 77
    fi

    if [ \"$cleanup_confirmed\" -eq 1 ]; then
      exit 0
    fi

    verify_process_identity() {
      if ! current_started_at=$(LC_ALL=C ps -o lstart= -p \"$pid\" 2>/dev/null); then
        printf 'remote process identity could not be read\n' >&2
        return 1
      fi
      if ! current_identity=$(printf '%s' \"$current_started_at\" | perl -e '
        local $/;
        my $value = <STDIN> // \"\";
        print unpack(\"H*\", $value);
      '); then
        printf 'remote process identity could not be encoded\n' >&2
        return 1
      fi
      if [ \"$current_identity\" != \"$process_identity\" ]; then
        printf 'remote process identity no longer matches the launch proof\n' >&2
        return 1
      fi
    }

    probe_process_group() {
      perl -MPOSIX -MErrno=ESRCH -e '
        my $pid = shift;
        exit 0 if POSIX::kill(-$pid, 0);
        exit 1 if $! == ESRCH;
        my $errno = 0 + $!;
        print STDERR \"remote process-group liveness probe failed: errno=$errno ($!)\\n\";
        exit 2;
      ' -- \"$pid\"
    }

    probe_process_group
    probe_status=$?
    case \"$probe_status\" in
      0)
        verify_process_identity || exit 80
        kill -TERM -- \"-$pid\" 2>/dev/null || {
          probe_process_group
          probe_status=$?
          case \"$probe_status\" in
            0) exit 70 ;;
            1) ;;
            *) exit 78 ;;
          esac
        }
        ;;
      1)
        ;;
      *)
        exit 78
        ;;
    esac
    if [ \"$probe_status\" -eq 0 ]; then
      sleep #{kill_delay_seconds}
    fi

    probe_process_group
    probe_status=$?
    case \"$probe_status\" in
      0)
        verify_process_identity || exit 80
        kill -KILL -- \"-$pid\" 2>/dev/null || {
          probe_process_group
          probe_status=$?
          case \"$probe_status\" in
            0) exit 71 ;;
            1) ;;
            *) exit 78 ;;
          esac
        }
        ;;
      1)
        ;;
      *)
        exit 78
        ;;
    esac

    kill_check=0
    while [ \"$kill_check\" -lt #{kill_check_attempts} ]; do
      probe_process_group
      probe_status=$?
      case \"$probe_status\" in
        0)
          sleep 0.025
          kill_check=$((kill_check + 1))
          ;;
        1)
          break
          ;;
        *)
          exit 78
          ;;
      esac
    done

    probe_process_group
    probe_status=$?
    case \"$probe_status\" in
      0)
        exit 72
        ;;
      1)
        completion_file=\"${pid_file}.stopped.$$\"
        if ! (umask 077 && printf 'stopped:%s:%s\n' \"$pid\" \"$process_identity\" > \"$completion_file\"); then
          rm -f \"$completion_file\"
          printf 'remote cleanup completion proof could not be written\n' >&2
          exit 79
        fi
        if ! mv -f \"$completion_file\" \"$pid_file\"; then
          rm -f \"$completion_file\"
          printf 'remote cleanup completion proof could not be installed\n' >&2
          exit 79
        fi
        ;;
      *)
        exit 78
        ;;
    esac
    """

    with {:ok, executable} <- ssh_executable(),
         {:ok, result} <-
           ProcessTree.run(executable, ssh_args(host, command),
             timeout_ms: timeout_ms,
             max_output_bytes: 64_000
           ) do
      case result do
        %{status: 0} ->
          :ok

        %{status: status, output: output} ->
          {:error, {:remote_process_cleanup_failed, host, status, output}}
      end
    else
      {:error, {:timeout, ^timeout_ms, output}} ->
        {:error, {:remote_process_cleanup_timeout, host, timeout_ms, output}}

      {:error, reason} ->
        {:error, {:remote_process_cleanup_start_failed, host, reason}}
    end
  end

  @spec remote_shell_command(String.t()) :: String.t()
  def remote_shell_command(command) when is_binary(command) do
    "bash -lc " <> shell_escape(command)
  end

  defp ssh_executable do
    case System.find_executable("ssh") do
      nil -> {:error, :ssh_not_found}
      executable -> {:ok, executable}
    end
  end

  defp ssh_args(host, command) do
    %{destination: destination, port: port} = parse_target(host)

    []
    |> maybe_put_config()
    |> Kernel.++(["-T"])
    |> maybe_put_port(port)
    |> Kernel.++([destination, remote_shell_command(command)])
  end

  defp maybe_put_line_option(port_opts, nil), do: port_opts
  defp maybe_put_line_option(port_opts, line_bytes), do: Keyword.put(port_opts, :line, line_bytes)

  defp maybe_put_config(args) do
    case System.get_env("SYMPHONY_SSH_CONFIG") do
      config_path when is_binary(config_path) and config_path != "" ->
        args ++ ["-F", config_path]

      _ ->
        args
    end
  end

  defp maybe_put_port(args, nil), do: args
  defp maybe_put_port(args, port), do: args ++ ["-p", port]

  defp parse_target(target) when is_binary(target) do
    trimmed_target = String.trim(target)

    # OpenSSH does not interpret bare "host:port" as "host + port"; it treats the
    # whole value as a hostname and leaves the port at 22. We split that shorthand
    # here so worker config can use "localhost:2222" without requiring ssh:// URIs.
    case Regex.run(~r/^(.*):(\d+)$/, trimmed_target, capture: :all_but_first) do
      [destination, port] ->
        if valid_port_destination?(destination) do
          %{destination: destination, port: port}
        else
          %{destination: trimmed_target, port: nil}
        end

      _ ->
        %{destination: trimmed_target, port: nil}
    end
  end

  defp valid_port_destination?(destination) when is_binary(destination) do
    destination != "" and
      (not String.contains?(destination, ":") or bracketed_host?(destination))
  end

  defp bracketed_host?(destination) when is_binary(destination) do
    # IPv6 literals contain ":" already, so we only accept additional ":port"
    # parsing when the host is explicitly bracketed, e.g. "[::1]:2222".
    String.contains?(destination, "[") and String.contains?(destination, "]")
  end

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end
end
