defmodule SymphonyElixir.ProcessTreeTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.ProcessTree

  test "bounded commands return output and status" do
    assert {:ok, %{status: 7, output: "hello\n"}} =
             ProcessTree.run(System.find_executable("bash"), ["-c", "printf 'hello\\n'; exit 7"], timeout_ms: 1_000)
  end

  test "timeouts reap the command process group and native descendants" do
    root = Path.join(System.tmp_dir!(), "process-tree-#{System.unique_integer([:positive])}")
    pid_file = Path.join(root, "child.pid")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    command = "sleep 60 & child=$!; printf '%s' \"$child\" > #{pid_file}; wait"

    assert {:error, {:timeout, 150, _output}} =
             ProcessTree.run(System.find_executable("bash"), ["-c", command], timeout_ms: 150)

    child_pid = pid_file |> File.read!() |> String.to_integer()
    refute os_process_alive?(child_pid)
  end

  test "multiple process groups share one bounded cleanup window" do
    ports =
      for _index <- 1..2 do
        {:ok, port} =
          ProcessTree.open_port(System.find_executable("bash"), ["-c", "trap '' TERM; sleep 60 & wait"])

        port
      end

    on_exit(fn -> Enum.each(ports, &ProcessTree.terminate_port(&1, 500)) end)
    os_pids = Enum.map(ports, fn port -> elem(:erlang.port_info(port, :os_pid), 1) end)
    started_at = System.monotonic_time(:millisecond)

    assert :ok = ProcessTree.terminate_os_process_trees(os_pids, 500)
    assert System.monotonic_time(:millisecond) - started_at < 750
    Enum.each(os_pids, fn os_pid -> refute os_process_alive?(os_pid) end)
  end

  test "cleanup fails closed when tracked processes or root groups survive" do
    assert {:error, {:process_tree_cleanup_unconfirmed, %{alive_pids: [1_234, 5_678], alive_process_groups: [1_234]}}} =
             ProcessTree.terminate_os_process_trees_for_test([1_234], 1,
               descendant_fetcher: fn 1_234 -> [5_678] end,
               probe: fn _target -> :alive end,
               signaler: fn _signal, _target -> :ok end
             )
  end

  test "cleanup fails closed when liveness probing is unavailable or denied" do
    for reason <- [:probe_unavailable, :eperm] do
      assert {:error, {:process_tree_probe_failed, 1_234, ^reason}} =
               ProcessTree.terminate_os_process_trees_for_test([1_234], 1,
                 descendant_fetcher: fn _root_pid -> [] end,
                 probe: fn _target -> {:error, reason} end,
                 signaler: fn _signal, _target -> :ok end
               )
    end
  end

  test "process group launcher validation fails when neither supported launcher exists" do
    assert {:error, {:process_group_launcher_not_found, ["missing-perl", "missing-setsid"]}} =
             ProcessTree.validate_launcher(perl: "missing-perl", setsid: "missing-setsid")
  end

  test "orchestrator refuses to start when runner preflight fails" do
    orchestrator_name = Module.concat(__MODULE__, :PreflightFailureOrchestrator)
    test_pid = self()
    Process.flag(:trap_exit, true)

    assert {:error, {:runner_capability_preflight_failed, :canary_failed}} =
             Orchestrator.start_link(
               name: orchestrator_name,
               runner_capability_preflight: fn ->
                 send(test_pid, :preflight_called)
                 {:error, :canary_failed}
               end
             )

    assert_receive :preflight_called
    refute Process.whereis(orchestrator_name)
  end

  test "orchestrator shutdown reaps recorded native process groups" do
    orchestrator_name = Module.concat(__MODULE__, :ShutdownCleanupOrchestrator)

    assert {:ok, supervisor} =
             Supervisor.start_link(
               [
                 {Orchestrator, name: orchestrator_name, runner_capability_preflight: fn -> :ok end}
               ],
               strategy: :one_for_one
             )

    orchestrator = Process.whereis(orchestrator_name)
    assert is_pid(orchestrator)

    {:ok, codex_port} =
      ProcessTree.open_port(System.find_executable("bash"), ["-c", "trap '' TERM; sleep 60 & wait"])

    {:os_pid, codex_os_pid} = :erlang.port_info(codex_port, :os_pid)

    on_exit(fn -> ProcessTree.terminate_port(codex_port, 500) end)

    :sys.replace_state(orchestrator, fn state ->
      running_entry = %{
        codex_app_server_pid: Integer.to_string(codex_os_pid),
        worker_host: nil
      }

      %{state | running: %{"issue-1" => running_entry}}
    end)

    assert :ok = Supervisor.stop(supervisor)
    refute os_process_alive?(codex_os_pid)
  end

  defp os_process_alive?(pid) do
    {_output, status} =
      System.cmd(System.find_executable("kill"), ["-0", Integer.to_string(pid)], stderr_to_stdout: true)

    status == 0
  end
end
