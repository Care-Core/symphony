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

    assert {:ok, orchestrator} =
             Orchestrator.start_link(name: orchestrator_name, runner_capability_preflight: fn -> :ok end)

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

    assert :ok = GenServer.stop(orchestrator, :normal, 2_000)
    refute os_process_alive?(codex_os_pid)
  end

  defp os_process_alive?(pid) do
    {_output, status} =
      System.cmd(System.find_executable("kill"), ["-0", Integer.to_string(pid)], stderr_to_stdout: true)

    status == 0
  end
end
