defmodule SymphonyElixir.SSHTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ProcessTree
  alias SymphonyElixir.SSH

  test "run/3 keeps bracketed IPv6 host:port targets intact" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-ipv6-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file)

    assert {:ok, {"", 0}} =
             SSH.run("root@[::1]:2200", "printf ok", stderr_to_stdout: true)

    trace = File.read!(trace_file)
    assert trace =~ "-T -p 2200 root@[::1] bash -lc"
    assert trace =~ "printf ok"
  end

  test "run/3 leaves unbracketed IPv6-style targets unchanged" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-ipv6-raw-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file)

    assert {:ok, {"", 0}} =
             SSH.run("::1:2200", "printf ok", stderr_to_stdout: true)

    trace = File.read!(trace_file)
    assert trace =~ "-T ::1:2200 bash -lc"
    refute trace =~ "-p 2200"
  end

  test "run/3 passes host:port targets through ssh -p" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")
    previous_ssh_config = System.get_env("SYMPHONY_SSH_CONFIG")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      restore_env("SYMPHONY_SSH_CONFIG", previous_ssh_config)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file)
    System.put_env("SYMPHONY_SSH_CONFIG", "/tmp/symphony-test-ssh-config")

    assert {:ok, {"", 0}} =
             SSH.run("localhost:2222", "echo ready", stderr_to_stdout: true)

    trace = File.read!(trace_file)
    assert trace =~ "-F /tmp/symphony-test-ssh-config"
    assert trace =~ "-T -p 2222 localhost bash -lc"
    assert trace =~ "echo ready"
  end

  test "run/3 keeps the user prefix when parsing user@host:port targets" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-user-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file)

    assert {:ok, {"", 0}} =
             SSH.run("root@127.0.0.1:2200", "printf ok", stderr_to_stdout: true)

    trace = File.read!(trace_file)
    assert trace =~ "-T -p 2200 root@127.0.0.1 bash -lc"
    assert trace =~ "printf ok"
  end

  test "run/3 returns an error when ssh is unavailable" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-missing-test-#{System.unique_integer([:positive])}")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(test_root)
    System.put_env("PATH", test_root)

    assert {:error, :ssh_not_found} = SSH.run("localhost", "printf ok")
  end

  test "start_port/3 supports binary output without line mode" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-port-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")
    previous_ssh_config = System.get_env("SYMPHONY_SSH_CONFIG")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      restore_env("SYMPHONY_SSH_CONFIG", previous_ssh_config)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file, """
    #!/bin/sh
    printf 'ARGV:%s\\n' "$*" >> "#{trace_file}"
    printf 'ready\\n'
    exit 0
    """)

    System.delete_env("SYMPHONY_SSH_CONFIG")

    assert {:ok, port} = SSH.start_port("localhost", "printf ok")
    assert is_port(port)
    wait_for_trace!(trace_file)

    trace = File.read!(trace_file)
    assert trace =~ "-T localhost bash -lc"
    refute trace =~ " -F "
  end

  test "start_port/3 supports line mode" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-line-port-test-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file, """
    #!/bin/sh
    printf 'ARGV:%s\\n' "$*" >> "#{trace_file}"
    printf 'ready\\n'
    exit 0
    """)

    assert {:ok, port} = SSH.start_port("localhost:2222", "printf ok", line: 256)
    assert is_port(port)
    wait_for_trace!(trace_file)

    trace = File.read!(trace_file)
    assert trace =~ "-T -p 2222 localhost bash -lc"
  end

  test "terminate_remote_process_tree/3 fails closed when the remote PID proof file is missing" do
    %{workspace: workspace} = install_remote_shell_fixture!("missing-pid")

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 73, output}} =
             SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

    assert output =~ "remote PID proof file is missing or unreadable"
  end

  test "terminate_remote_process_tree/3 fails closed when the remote PID proof file is unreadable" do
    %{pid_file: pid_file, workspace: workspace} = install_remote_shell_fixture!("unreadable-pid")
    File.mkdir!(pid_file)

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 74, output}} =
             SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

    assert output =~ "remote PID proof file is unreadable"
  end

  test "terminate_remote_process_tree/3 fails closed when the remote PID proof file is empty" do
    %{pid_file: pid_file, workspace: workspace} = install_remote_shell_fixture!("empty-pid")
    File.write!(pid_file, "")

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 75, output}} =
             SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

    assert output =~ "remote PID proof file is empty"
    assert File.exists?(pid_file)
  end

  test "terminate_remote_process_tree/3 fails closed when the remote PID proof file is malformed" do
    %{pid_file: pid_file, workspace: workspace} = install_remote_shell_fixture!("malformed-pid")
    File.write!(pid_file, "123x\n")

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 76, output}} =
             SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

    assert output =~ "remote PID proof file is nonnumeric"
    assert File.exists?(pid_file)
  end

  test "terminate_remote_process_tree/3 rejects unsafe numeric PID proofs before signaling" do
    %{pid_file: pid_file, signal_trace_file: signal_trace_file, workspace: workspace} =
      install_remote_shell_fixture!("unsafe-numeric-pid", protect_signals: true)

    for pid <- ["0", "1", "00", "01", "2147483648", "999999999999999999999999999999"] do
      File.write!(pid_file, pid <> "\n")

      assert {:error, {:remote_process_cleanup_failed, "worker.example", 77, output}} =
               SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

      assert output =~ "remote PID proof file is outside the safe process-group range"
      assert File.read!(pid_file) == pid <> "\n"
      refute File.exists?(signal_trace_file)
    end
  end

  test "terminate_remote_process_tree/3 fails closed on liveness probe errors" do
    %{pid_file: pid_file, workspace: workspace} =
      install_remote_shell_fixture!("probe-error", probe_error: true)

    File.write!(pid_file, "2147483647\n")

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 78, output}} =
             SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)

    assert output =~ "simulated liveness probe failure"
    assert File.read!(pid_file) == "2147483647\n"
  end

  test "terminate_remote_process_tree/3 terminates the live remote process group" do
    %{pid_file: pid_file, test_root: test_root, workspace: workspace} =
      install_remote_shell_fixture!("live-pid")

    ready_file = Path.join(test_root, "process.ready")

    assert {:ok, port} =
             ProcessTree.open_port(
               System.find_executable("bash"),
               ["-c", "printf ready > \"$1\"; exec sleep 60", "bash", ready_file]
             )

    on_exit(fn -> ProcessTree.terminate_port(port, 500) end)
    wait_for_trace!(ready_file)
    {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)
    File.write!(pid_file, "#{os_pid}\n")

    assert :ok = SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)
    refute File.exists?(pid_file)
    refute os_process_alive?(os_pid)
  end

  test "terminate_remote_process_tree/3 removes a stale numeric remote PID proof file" do
    %{pid_file: pid_file, workspace: workspace} = install_remote_shell_fixture!("stale-pid")
    File.write!(pid_file, "2147483647\n")

    assert :ok = SSH.terminate_remote_process_tree("worker.example", workspace, 1_000)
    refute File.exists?(pid_file)
  end

  test "terminate_remote_process_tree/3 returns remote command failures" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-terminate-failure-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file, """
    #!/bin/sh
    printf 'cleanup refused\n'
    exit 23
    """)

    assert {:error, {:remote_process_cleanup_failed, "worker.example", 23, output}} =
             SSH.terminate_remote_process_tree("worker.example", "/srv/workspaces/MT-1", 1_000)

    assert output =~ "cleanup refused"
  end

  test "terminate_remote_process_tree/3 bounds cleanup and kills a timed-out ssh process" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-terminate-timeout-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file, """
    #!/bin/sh
    printf 'started\n' >> "#{trace_file}"
    sleep 60
    printf 'finished\n' >> "#{trace_file}"
    """)

    started_at_ms = System.monotonic_time(:millisecond)

    assert {:error, {:remote_process_cleanup_timeout, "worker.example", 100, _output}} =
             SSH.terminate_remote_process_tree("worker.example", "/srv/workspaces/MT-1", 100)

    elapsed_ms = System.monotonic_time(:millisecond) - started_at_ms
    assert elapsed_ms < 1_000

    Process.sleep(150)

    if File.exists?(trace_file) do
      refute File.read!(trace_file) =~ "finished"
    end
  end

  test "terminate_remote_process_tree/3 surfaces process-launch failures" do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-terminate-start-failure-#{System.unique_integer([:positive])}")
    trace_file = Path.join(test_root, "ssh.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    install_fake_ssh!(test_root, trace_file)
    System.put_env("PATH", Path.join(test_root, "bin"))

    assert {:error, {:remote_process_cleanup_start_failed, "worker.example", {:process_group_launcher_not_found, ["perl", "setsid"]}}} =
             SSH.terminate_remote_process_tree("worker.example", "/srv/workspaces/MT-1", 1_000)
  end

  test "remote_shell_command/1 escapes embedded single quotes" do
    assert SSH.remote_shell_command("printf 'hello'") ==
             "bash -lc 'printf '\"'\"'hello'\"'\"''"
  end

  defp install_fake_ssh!(test_root, trace_file, script \\ nil) do
    fake_bin_dir = Path.join(test_root, "bin")
    fake_ssh = Path.join(fake_bin_dir, "ssh")

    File.mkdir_p!(fake_bin_dir)

    File.write!(
      fake_ssh,
      script ||
        """
        #!/bin/sh
        printf 'ARGV:%s\\n' "$*" >> "#{trace_file}"
        exit 0
        """
    )

    File.chmod!(fake_ssh, 0o755)
    System.put_env("PATH", fake_bin_dir <> ":" <> (System.get_env("PATH") || ""))
  end

  defp install_remote_shell_fixture!(name, opts \\ []) do
    test_root = Path.join(System.tmp_dir!(), "symphony-ssh-#{name}-#{System.unique_integer([:positive])}")
    workspace = Path.join(test_root, "workspace")
    pid_file = Path.join(workspace, ".symphony-codex-app-server.pid")
    signal_trace_file = Path.join(test_root, "signals.trace")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(workspace)
    probe_error? = Keyword.get(opts, :probe_error, false)

    if probe_error? do
      install_probe_error_perl!(test_root)
    end

    install_executing_fake_ssh!(
      test_root,
      Keyword.get(opts, :protect_signals, false),
      probe_error?,
      signal_trace_file
    )

    %{
      pid_file: pid_file,
      signal_trace_file: signal_trace_file,
      test_root: test_root,
      workspace: workspace
    }
  end

  defp install_executing_fake_ssh!(test_root, protect_signals?, probe_error?, signal_trace_file) do
    fake_bin_dir = Path.join(test_root, "bin")
    fake_ssh = Path.join(fake_bin_dir, "ssh")

    File.mkdir_p!(fake_bin_dir)

    signal_guard =
      if protect_signals? do
        """
        kill() {
          printf '%s\\n' "$*" >> "#{signal_trace_file}"
          return 1
        }
        export -f kill
        """
      else
        ""
      end

    perl_guard =
      if probe_error? do
        """
        perl() {
          "#{Path.join(fake_bin_dir, "perl")}" "$@"
        }
        export -f perl
        """
      else
        ""
      end

    File.write!(fake_ssh, """
    #!/bin/bash
    #{signal_guard}
    #{perl_guard}
    for arg in "$@"; do
      remote_command=$arg
    done
    exec /bin/sh -c "$remote_command"
    """)

    File.chmod!(fake_ssh, 0o755)
    System.put_env("PATH", fake_bin_dir <> ":" <> (System.get_env("PATH") || ""))
  end

  defp install_probe_error_perl!(test_root) do
    fake_perl = Path.join([test_root, "bin", "perl"])
    real_perl = System.find_executable("perl")

    File.mkdir_p!(Path.dirname(fake_perl))

    File.write!(fake_perl, """
    #!/bin/sh
    if [ "$1" = "-MPOSIX" ] && [ "$2" = "-MErrno=ESRCH" ]; then
      printf 'simulated liveness probe failure\n' >&2
      exit 2
    fi
    exec "#{real_perl}" "$@"
    """)

    File.chmod!(fake_perl, 0o755)
  end

  defp wait_for_trace!(trace_file, attempts \\ 80)
  defp wait_for_trace!(trace_file, 0), do: flunk("timed out waiting for fake ssh trace at #{trace_file}")

  defp wait_for_trace!(trace_file, attempts) do
    if File.exists?(trace_file) and File.read!(trace_file) != "" do
      :ok
    else
      Process.sleep(25)
      wait_for_trace!(trace_file, attempts - 1)
    end
  end

  defp os_process_alive?(pid) do
    {_output, status} =
      System.cmd(System.find_executable("kill"), ["-0", Integer.to_string(pid)], stderr_to_stdout: true)

    status == 0
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
