defmodule SymphonyElixir.TokenBudgetTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.ProcessTree

  test "config selects the smallest matching case-insensitive label limit" do
    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: 1_000,
      codex_input_token_limits_by_label: %{"Backend" => 800, "URGENT" => 250}
    )

    issue = %Issue{labels: [" backend ", "urgent"]}
    assert Config.input_token_limit_for_issue(issue) == 250
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == 1_000

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: nil,
      codex_input_token_limits_by_label: %{"Backend" => 400}
    )

    assert Config.input_token_limit_for_issue(%Issue{labels: ["BACKEND"]}) == 400
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == nil
  end

  test "config rejects non-positive limits and invalid warning ratios" do
    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_limit: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limit"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{"backend" => -1}
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limits_by_label"

    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_warning_ratio: 1.0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_warning_ratio"
  end

  test "warning threshold with no live steering channel creates a durable hold" do
    {pid, issue, worker_pid} = start_budget_orchestrator("warning", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)

    send_token_update(pid, issue.id, 69)
    assert [running] = Orchestrator.snapshot(pid, 1_000).running
    assert running.input_token_warning_status == nil

    send_token_update(pid, issue.id, 70)
    snapshot = Orchestrator.snapshot(pid, 30_000)
    assert snapshot.running == []
    assert snapshot.retrying == []

    assert [%{reason: "input_token_warning_unsupported", limit: 100, observed_tokens: 70}] =
             snapshot.held

    refute Process.alive?(worker_pid)
  end

  test "modern thread token-usage warning also fails closed without steering" do
    {pid, issue, worker_pid} = start_budget_orchestrator("modern-warning", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)

    send_modern_token_update(pid, issue.id, 70)

    snapshot = Orchestrator.snapshot(pid, 30_000)
    assert snapshot.running == []
    assert [%{reason: "input_token_warning_unsupported", observed_tokens: 70}] = snapshot.held
    refute Process.alive?(worker_pid)
  end

  test "a rejected steering response holds the run before the hard limit" do
    {pid, issue, worker_pid} = start_budget_orchestrator("rejected-warning", 100)
    workspace = Path.join(Config.settings!().workspace.root, issue.identifier)
    marker = Path.join(workspace, "preserve-me")
    File.mkdir_p!(workspace)
    File.write!(marker, "kept")

    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      workspace_path: workspace,
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    assert [%{input_token_warning_status: "requested"}] = Orchestrator.snapshot(pid, 1_000).running

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :token_budget_warning_unsupported,
         payload: %{"code" => -32_602, "message" => "invalid params"},
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = Orchestrator.snapshot(pid, 30_000)
    assert snapshot.running == []
    assert snapshot.retrying == []

    assert [%{reason: "input_token_warning_unsupported", limit: 100, observed_tokens: 70}] =
             snapshot.held

    refute Process.alive?(worker_pid)
    assert File.read!(marker) == "kept"
  end

  test "an unacknowledged steering request holds the run after its deadline" do
    {pid, issue, worker_pid} = start_budget_orchestrator("unacknowledged-warning", 100)
    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    state = :sys.get_state(pid)
    timeout_token = state.running[issue.id].input_token_warning_ack_token
    assert is_reference(timeout_token)

    send(pid, {:input_token_warning_ack_timeout, issue.id, timeout_token})

    snapshot = Orchestrator.snapshot(pid, 30_000)
    assert snapshot.running == []
    assert snapshot.retrying == []
    assert [%{reason: "input_token_warning_unsupported", observed_tokens: 70}] = snapshot.held
    refute Process.alive?(worker_pid)
  end

  test "acknowledgement deadline defers while the protocol reader executes a tool" do
    {pid, issue, worker_pid} = start_budget_orchestrator("busy-reader-warning", 100)
    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])
    on_exit(fn -> ProcessTree.terminate_port(codex_port, 500) end)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    first_timeout_token = :sys.get_state(pid).running[issue.id].input_token_warning_ack_token

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :tool_call_started,
         payload: %{"method" => "item/tool/call"},
         timestamp: DateTime.utc_now()
       }}
    )

    send(pid, {:input_token_warning_ack_timeout, issue.id, first_timeout_token})
    busy_entry = :sys.get_state(pid).running[issue.id]
    assert busy_entry.input_token_warning_status == "requested"
    assert busy_entry.input_token_warning_reader_busy == true
    refute busy_entry.input_token_warning_ack_token == first_timeout_token

    busy_timeout_token = busy_entry.input_token_warning_ack_token

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :tool_call_completed,
         payload: %{"success" => true},
         timestamp: DateTime.utc_now()
       }}
    )

    ready_entry = :sys.get_state(pid).running[issue.id]
    assert ready_entry.input_token_warning_reader_busy == false
    refute ready_entry.input_token_warning_ack_token == busy_timeout_token

    send(pid, {:input_token_warning_ack_timeout, issue.id, busy_timeout_token})

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :token_budget_warning_delivered,
         payload: %{"turnId" => "turn-warning"},
         timestamp: DateTime.utc_now()
       }}
    )

    assert [%{input_token_warning_status: "delivered"}] =
             Orchestrator.snapshot(pid, 1_000).running

    assert Orchestrator.snapshot(pid, 1_000).held == []
    assert Process.alive?(worker_pid)
  end

  test "worker exit while warning acknowledgement is pending creates a hold instead of a retry" do
    install_fake_ssh!("pending-warning-exit", "exit 0\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("pending-warning-exit", 100)
    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])
    on_exit(fn -> ProcessTree.terminate_port(codex_port, 500) end)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}",
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    assert [%{input_token_warning_status: "requested"}] = Orchestrator.snapshot(pid, 1_000).running

    running_ref = :sys.get_state(pid).running[issue.id].ref
    Process.exit(worker_pid, :shutdown)
    send(pid, {:DOWN, running_ref, :process, worker_pid, :shutdown})

    assert_eventually(fn ->
      case Orchestrator.snapshot(pid, 1_000) do
        %{running: [], retrying: [], held: held} ->
          match?(
            [
              %{
                reason: "input_token_warning_unsupported",
                observed_tokens: 70,
                cleanup_pending: false
              }
            ],
            held
          )

        _ ->
          false
      end
    end)
  end

  test "worker-exit warning cleanup retries from durable proof after the running entry is gone" do
    {fake_ssh, _fake_root} = install_fake_ssh!("pending-warning-exit-cleanup", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("pending-warning-exit-cleanup", 100)
    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])
    on_exit(fn -> ProcessTree.terminate_port(codex_port, 500) end)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}",
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    assert [%{input_token_warning_status: "requested"}] = Orchestrator.snapshot(pid, 1_000).running

    running_ref = :sys.get_state(pid).running[issue.id].ref
    Process.exit(worker_pid, :shutdown)
    send(pid, {:DOWN, running_ref, :process, worker_pid, :shutdown})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      state.running == %{} and state.hold_store_available == false and
        state.holds[issue.id].cleanup_pending == true
    end)

    paused_state = :sys.get_state(pid)
    Process.cancel_timer(paused_state.hold_state_persist_retry_timer_ref)
    File.write!(fake_ssh, "#!/bin/sh\nexit 0\n")

    send(pid, {:hold_state_persist_retry, paused_state.hold_state_persist_retry_token})

    assert_eventually(fn ->
      recovered_state = :sys.get_state(pid)

      recovered_state.running == %{} and recovered_state.hold_store_available == true and
        recovered_state.holds[issue.id].cleanup_pending == false
    end)
  end

  test "a warning hold stops immediately and pauses dispatch while persistence retries" do
    {pid, issue, worker_pid} = start_budget_orchestrator("warning-hold-retry", 100)
    state_file = Path.join(Config.settings!().workspace.root, ".symphony-holds.json")
    File.mkdir_p!(state_file)

    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_token_update(pid, issue.id, 70)

    state = :sys.get_state(pid)
    assert state.running == %{}
    assert state.hold_store_available == false
    assert state.holds[issue.id].reason == "input_token_warning_unsupported"
    retry_token = state.hold_state_persist_retry_token
    assert is_reference(retry_token)
    refute Process.alive?(worker_pid)

    send(pid, :run_poll_cycle)
    assert :sys.get_state(pid).running == %{}

    retry_issue = %Issue{
      id: "issue-dispatch-paused",
      identifier: "MT-DISPATCH-PAUSED",
      title: "Dispatch remains paused",
      state: "In Progress",
      labels: []
    }

    queued_retry_token = make_ref()

    :sys.replace_state(pid, fn paused_state ->
      retry_entry = %{
        attempt: 1,
        retry_token: queued_retry_token,
        identifier: retry_issue.identifier,
        worker_host: nil,
        workspace_path: nil
      }

      %{
        paused_state
        | retry_attempts: Map.put(paused_state.retry_attempts, retry_issue.id, retry_entry),
          claimed: MapSet.put(paused_state.claimed, retry_issue.id)
      }
    end)

    send(pid, {:retry_issue, retry_issue.id, queued_retry_token})

    assert_eventually(fn ->
      paused_state = :sys.get_state(pid)

      Map.has_key?(paused_state.retry_attempts, retry_issue.id) and
        not Map.has_key?(paused_state.running, retry_issue.id)
    end)

    File.rmdir!(state_file)
    send(pid, {:hold_state_persist_retry, retry_token})

    snapshot = Orchestrator.snapshot(pid, 30_000)
    assert snapshot.running == []
    assert [%{reason: "input_token_warning_unsupported", observed_tokens: 70}] = snapshot.held
    assert :sys.get_state(pid).hold_store_available == true
    assert File.regular?(state_file)
    refute Process.alive?(worker_pid)
  end

  test "hard-limit enforcement stops even when hold persistence is unavailable" do
    {pid, issue, worker_pid} = start_budget_orchestrator("hard-limit-hold-retry", 100)
    state_file = Path.join(Config.settings!().workspace.root, ".symphony-holds.json")
    File.mkdir_p!(state_file)

    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_token_update(pid, issue.id, 100)

    state = :sys.get_state(pid)
    assert state.running == %{}
    assert state.hold_store_available == false
    assert state.holds[issue.id].reason == "input_token_limit"
    assert state.holds[issue.id].observed_tokens == 100
    refute Process.alive?(worker_pid)
  end

  test "dispatch stays paused until quarantined cleanup succeeds after persistence recovery" do
    {fake_ssh, _fake_root} = install_fake_ssh!("outage-cleanup-retry", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("outage-cleanup-retry", 100)
    state_file = Path.join(Config.settings!().workspace.root, ".symphony-holds.json")
    File.mkdir_p!(state_file)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}"
    )

    send_token_update(pid, issue.id, 70)
    paused_state = :sys.get_state(pid)
    assert paused_state.hold_store_available == false
    assert Map.has_key?(paused_state.running, issue.id)
    assert paused_state.holds[issue.id].cleanup_pending == true
    Process.cancel_timer(paused_state.hold_state_persist_retry_timer_ref)

    File.rmdir!(state_file)
    send(pid, {:hold_state_persist_retry, paused_state.hold_state_persist_retry_token})

    assert_eventually(fn ->
      retry_state = :sys.get_state(pid)
      retry_state.hold_store_available == false and Map.has_key?(retry_state.running, issue.id)
    end)

    cleanup_retry_state = :sys.get_state(pid)
    Process.cancel_timer(cleanup_retry_state.hold_state_persist_retry_timer_ref)
    File.write!(fake_ssh, "#!/bin/sh\nexit 0\n")

    send(
      pid,
      {:hold_state_persist_retry, cleanup_retry_state.hold_state_persist_retry_token}
    )

    assert_eventually(fn ->
      recovered_state = :sys.get_state(pid)

      recovered_state.hold_store_available == true and
        recovered_state.running == %{} and
        recovered_state.holds[issue.id].cleanup_pending == false
    end)

    refute Process.alive?(worker_pid)
  end

  test "a delivered steering response keeps the checkpointed run active" do
    {pid, issue, worker_pid} = start_budget_orchestrator("delivered-warning", 100)
    command = "while IFS= read -r _line; do :; done"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])
    on_exit(fn -> ProcessTree.terminate_port(codex_port, 500) end)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      codex_app_server_port: codex_port,
      thread_id: "thread-warning",
      turn_id: "turn-warning"
    )

    send_token_update(pid, issue.id, 70)
    assert [%{input_token_warning_status: "requested"}] = Orchestrator.snapshot(pid, 1_000).running

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :token_budget_warning_delivered,
         payload: %{"turnId" => "turn-warning"},
         timestamp: DateTime.utc_now()
       }}
    )

    assert [%{input_token_warning_status: "delivered"}] =
             Orchestrator.snapshot(pid, 1_000).running

    assert Orchestrator.snapshot(pid, 1_000).held == []
    assert Process.alive?(worker_pid)
  end

  test "exact hard limit stops the process tree, suppresses retries, and preserves workspace" do
    {pid, issue, worker_pid} = start_budget_orchestrator("hard-limit", 100)
    workspace = Path.join(Config.settings!().workspace.root, issue.identifier)
    marker = Path.join(workspace, "preserve-me")
    File.mkdir_p!(workspace)
    File.write!(marker, "kept")

    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", "sleep 60"])
    {:os_pid, codex_os_pid} = :erlang.port_info(codex_port, :os_pid)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      workspace_path: workspace,
      codex_app_server_pid: Integer.to_string(codex_os_pid)
    )

    send_token_update(pid, issue.id, 100)
    snapshot = Orchestrator.snapshot(pid, 2_000)

    assert snapshot.running == []
    assert snapshot.retrying == []

    assert [hold] = snapshot.held
    assert hold.reason == "input_token_limit"
    assert hold.limit == 100
    assert hold.observed_tokens == 100

    assert {:ok, issue_payload} =
             SymphonyElixirWeb.Presenter.issue_payload(issue.identifier, pid, 1_000)

    assert issue_payload.status == "held"
    assert issue_payload.hold.reason == "input_token_limit"
    assert issue_payload.hold.limit == 100
    assert issue_payload.hold.observed_tokens == 100

    refute Process.alive?(worker_pid)
    refute os_process_alive?(codex_os_pid)
    assert File.read!(marker) == "kept"

    send(pid, {:DOWN, make_ref(), :process, worker_pid, :shutdown})
    assert Orchestrator.snapshot(pid, 1_000).retrying == []
  end

  test "modern thread token-usage notifications enforce the exact hard limit" do
    {pid, issue, worker_pid} = start_budget_orchestrator("modern-hard-limit", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)

    send_modern_token_update(pid, issue.id, 100)

    snapshot = Orchestrator.snapshot(pid, 1_000)
    assert snapshot.running == []
    assert [%{reason: "input_token_limit", limit: 100, observed_tokens: 100}] = snapshot.held
    refute Process.alive?(worker_pid)
  end

  test "explicit resume clears a hold and unknown controls are rejected" do
    {pid, issue, worker_pid} = start_budget_orchestrator("resume", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_token_update(pid, issue.id, 100)

    assert {:ok, %{resumed: true}} = Orchestrator.resume_issue(issue.identifier, pid)
    assert Orchestrator.snapshot(pid, 1_000).held == []

    assert {:error, :issue_not_found} = Orchestrator.stop_issue("UNKNOWN-1", pid)
    assert {:error, :issue_not_found} = Orchestrator.resume_issue("UNKNOWN-1", pid)
  end

  test "manual stop returns after running ends and preserves the workspace" do
    {pid, issue, worker_pid} = start_budget_orchestrator("manual-stop", 100)
    workspace = Path.join(Config.settings!().workspace.root, issue.identifier)
    marker = Path.join(workspace, "preserve-manual")
    File.mkdir_p!(workspace)
    File.write!(marker, "kept")
    put_running_entry(pid, issue, worker_pid, workspace_path: workspace)

    assert {:ok, hold} = Orchestrator.stop_issue(issue.identifier, pid)
    assert hold.reason == "manual_stop"
    assert Orchestrator.snapshot(pid, 1_000).running == []
    refute Process.alive?(worker_pid)
    assert File.read!(marker) == "kept"
  end

  test "running stop persists pending cleanup before interrupt and cleanup" do
    {fake_ssh, _fake_root} = install_fake_ssh!("persist-before-cleanup", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("persist-before-cleanup", 100, 1_000)
    workspace_root = Config.settings!().workspace.root
    state_file = Path.join(workspace_root, ".symphony-holds.json")
    interrupt_trace = Path.join(workspace_root, "interrupt-seen")
    cleanup_trace = Path.join(workspace_root, "cleanup-state.json")

    command = "IFS= read -r line; touch '#{interrupt_trace}'"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])

    File.write!(
      fake_ssh,
      "#!/bin/sh\nwhile [ ! -e '#{interrupt_trace}' ]; do sleep 0.01; done\ncp '#{state_file}' '#{cleanup_trace}'\nexit 0\n"
    )

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}",
      codex_app_server_port: codex_port,
      thread_id: "thread-stop",
      turn_id: "turn-stop"
    )

    assert {:ok, %{cleanup_pending: false}} = Orchestrator.stop_issue(issue.identifier, pid)
    assert File.exists?(interrupt_trace)

    assert %{
             "holds" => [
               %{"identifier" => identifier, "cleanup_pending" => true}
             ]
           } = cleanup_trace |> File.read!() |> Jason.decode!()

    assert identifier == issue.identifier
  end

  test "a later issue-state change releases a hold" do
    {pid, issue, worker_pid} = start_budget_orchestrator("state-change", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_token_update(pid, issue.id, 100)
    assert [_hold] = Orchestrator.snapshot(pid, 1_000).held

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [
      %{issue | state: "Human Review"}
    ])

    send(pid, :run_poll_cycle)
    assert_eventually(fn -> Orchestrator.snapshot(pid, 1_000).held == [] end)
  end

  test "holds survive restart and release only after a verified tracker-state change" do
    {pid, issue, worker_pid} = start_budget_orchestrator("restart-state-change", 100)
    workspace_root = Config.settings!().workspace.root
    state_file = Path.join(workspace_root, ".symphony-holds.json")

    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_modern_token_update(pid, issue.id, 100)
    assert [_hold] = Orchestrator.snapshot(pid, 1_000).held
    assert File.exists?(state_file)
    assert Bitwise.band(File.stat!(state_file).mode, 0o777) == 0o600

    :ok = GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert [%{identifier: identifier, issue_state: "In Progress"}] =
             Orchestrator.snapshot(restarted_pid, 1_000).held

    assert identifier == issue.identifier

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    send(restarted_pid, :run_poll_cycle)
    assert_eventually(fn -> length(Orchestrator.snapshot(restarted_pid, 1_000).held) == 1 end)

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [%{issue | state: "Human Review"}])
    send(restarted_pid, :run_poll_cycle)
    assert_eventually(fn -> Orchestrator.snapshot(restarted_pid, 1_000).held == [] end)

    assert %{"version" => 1, "holds" => []} = state_file |> File.read!() |> Jason.decode!()
  end

  test "restart restoration keeps pending cleanup held across tracker changes" do
    install_fake_ssh!("restart-pending", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("restart-pending", 100)

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}"
    )

    assert {:error, :cleanup_failed} = Orchestrator.stop_issue(issue.identifier, pid)
    assert [%{cleanup_pending: true}] = Orchestrator.snapshot(pid, 1_000).held

    :ok = GenServer.stop(pid)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [%{issue | state: "Human Review"}])
    restarted_pid = start_replacement_orchestrator()
    send(restarted_pid, :run_poll_cycle)

    assert_eventually(fn ->
      match?([%{cleanup_pending: true}], Orchestrator.snapshot(restarted_pid, 1_000).held)
    end)
  end

  test "an explicit resume releases a restored durable hold" do
    {pid, issue, worker_pid} = start_budget_orchestrator("restart-resume", 100)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_modern_token_update(pid, issue.id, 100)
    assert [_hold] = Orchestrator.snapshot(pid, 1_000).held

    :ok = GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()
    assert [_hold] = Orchestrator.snapshot(restarted_pid, 1_000).held

    assert {:ok, %{resumed: true}} = Orchestrator.resume_issue(issue.identifier, restarted_pid)
    assert Orchestrator.snapshot(restarted_pid, 1_000).held == []
  end

  test "resume retries stored cleanup proof and releases only after confirmed cleanup" do
    {fake_ssh, _fake_root} = install_fake_ssh!("resume-pending", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("resume-pending", 100)
    workspace_root = Config.settings!().workspace.root
    cleanup_trace = Path.join(workspace_root, "resume-cleanup-args")
    remote_workspace = "/srv/workspaces/#{issue.identifier}"

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: remote_workspace
    )

    assert {:error, :cleanup_failed} = Orchestrator.stop_issue(issue.identifier, pid)
    :ok = GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    File.write!(fake_ssh, "#!/bin/sh\nprintf '%s\\n' \"$@\" > '#{cleanup_trace}'\nexit 17\n")
    assert {:error, :cleanup_failed} = Orchestrator.resume_issue(issue.identifier, restarted_pid)
    assert [%{cleanup_pending: true}] = Orchestrator.snapshot(restarted_pid, 1_000).held
    assert File.read!(cleanup_trace) =~ Path.join(remote_workspace, ".symphony-codex-app-server.pid")

    File.write!(fake_ssh, "#!/bin/sh\nprintf '%s\\n' \"$@\" > '#{cleanup_trace}'\nexit 0\n")
    assert {:ok, %{resumed: true}} = Orchestrator.resume_issue(issue.identifier, restarted_pid)
    assert Orchestrator.snapshot(restarted_pid, 1_000).held == []
  end

  test "persistence recovery durably confirms an already successful cleanup" do
    {fake_ssh, _fake_root} = install_fake_ssh!("cleanup-persist-failure", "exit 17\n")
    {pid, issue, worker_pid} = start_budget_orchestrator("cleanup-persist-failure", 100)
    workspace_root = Config.settings!().workspace.root
    state_file = Path.join(workspace_root, ".symphony-holds.json")

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}"
    )

    assert {:error, :cleanup_failed} = Orchestrator.stop_issue(issue.identifier, pid)
    :ok = GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    File.write!(fake_ssh, "#!/bin/sh\nexit 0\n")
    File.rm!(state_file)
    File.mkdir_p!(state_file)

    assert {:error, :hold_state_unavailable} =
             Orchestrator.resume_issue(issue.identifier, restarted_pid)

    assert [%{cleanup_pending: true}] = Orchestrator.snapshot(restarted_pid, 1_000).held

    paused_state = :sys.get_state(restarted_pid)
    assert paused_state.hold_store_available == false
    Process.cancel_timer(paused_state.hold_state_persist_retry_timer_ref)

    File.rmdir!(state_file)
    send(restarted_pid, {:hold_state_persist_retry, paused_state.hold_state_persist_retry_token})

    assert_eventually(fn -> :sys.get_state(restarted_pid).hold_store_available == true end)
    assert File.regular?(state_file)
    assert [%{cleanup_pending: false}] = Orchestrator.snapshot(restarted_pid, 1_000).held
  end

  test "manual stop of a retry stores a non-pending hold without process proof" do
    {pid, issue, _worker_pid} = start_budget_orchestrator("retry-stop", 100)

    :sys.replace_state(pid, fn state ->
      retry = %{
        identifier: issue.identifier,
        attempt: 2,
        due_at_ms: System.monotonic_time(:millisecond) + 60_000,
        timer_ref: nil,
        worker_host: nil,
        workspace_path: "/tmp/#{issue.identifier}"
      }

      %{
        state
        | running: %{},
          retry_attempts: %{issue.id => retry},
          claimed: MapSet.put(state.claimed, issue.id)
      }
    end)

    assert {:ok, hold} = Orchestrator.stop_issue(issue.identifier, pid)
    assert hold.cleanup_pending == false
    assert hold.codex_app_server_pid == nil

    state_file = Path.join(Config.settings!().workspace.root, ".symphony-holds.json")

    assert %{
             "holds" => [
               %{"cleanup_pending" => false, "codex_app_server_pid" => nil}
             ]
           } = state_file |> File.read!() |> Jason.decode!()
  end

  test "startup fails closed when durable hold state is corrupt or unreadable" do
    workspace_root =
      Path.join(System.tmp_dir!(), "symphony-budget-corrupt-holds-#{System.unique_integer([:positive])}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root
    )

    File.mkdir_p!(workspace_root)
    state_file = Path.join(workspace_root, ".symphony-holds.json")
    File.write!(state_file, "not-json")
    File.chmod!(state_file, 0o600)

    assert {:error, {:hold_state_load_failed, {:hold_state_invalid, ^state_file, _reason}}} =
             GenServer.start(Orchestrator, [], name: unique_orchestrator_name())

    File.rm!(state_file)
    File.mkdir_p!(state_file)

    assert {:error, {:hold_state_load_failed, {:hold_state_invalid_file, ^state_file, :directory}}} =
             GenServer.start(Orchestrator, [], name: unique_orchestrator_name())

    File.rm_rf(workspace_root)
  end

  test "hold persistence failure leaves the running worker uninterrupted" do
    {pid, issue, worker_pid} = start_budget_orchestrator("persistence-failure", 100)
    workspace_root = Config.settings!().workspace.root
    state_file = Path.join(workspace_root, ".symphony-holds.json")
    interrupt_trace = Path.join(workspace_root, "interrupt.json")
    File.mkdir_p!(state_file)

    command = "IFS= read -r line; printf '%s' \"$line\" > '#{interrupt_trace}'"
    {:ok, codex_port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])

    put_running_entry(pid, issue, worker_pid,
      codex_app_server_port: codex_port,
      thread_id: "thread-1",
      turn_id: "turn-1"
    )

    assert {:error, :hold_state_unavailable} = Orchestrator.stop_issue(issue.identifier, pid)
    assert [%{identifier: identifier}] = Orchestrator.snapshot(pid, 1_000).running
    assert identifier == issue.identifier
    assert Orchestrator.snapshot(pid, 1_000).held == []
    assert Process.alive?(worker_pid)
    Process.sleep(50)
    refute File.exists?(interrupt_trace)
  end

  test "remote cleanup failure keeps the persisted hold and live running entry" do
    {_fake_ssh, _test_root} = install_fake_ssh!("remote-cleanup", "exit 17\n")

    {pid, issue, worker_pid} = start_budget_orchestrator("remote-cleanup-failure", 100)

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}"
    )

    assert {:error, :cleanup_failed} = Orchestrator.stop_issue(issue.identifier, pid)
    snapshot = Orchestrator.snapshot(pid, 1_000)

    assert [%{identifier: identifier}] = snapshot.running
    assert identifier == issue.identifier
    assert [%{identifier: ^identifier, reason: "manual_stop", cleanup_pending: true}] = snapshot.held
    assert snapshot.retrying == []
    assert MapSet.member?(:sys.get_state(pid).claimed, issue.id)
    assert Process.alive?(worker_pid)

    state_file = Path.join(Config.settings!().workspace.root, ".symphony-holds.json")

    assert %{
             "holds" => [
               %{"identifier" => ^identifier, "cleanup_pending" => true}
             ]
           } = state_file |> File.read!() |> Jason.decode!()

    running_ref = :sys.get_state(pid).running[issue.id].ref
    send(pid, {:DOWN, running_ref, :process, worker_pid, :shutdown})

    assert_eventually(fn -> Orchestrator.snapshot(pid, 1_000).running == [] end)
    assert Orchestrator.snapshot(pid, 1_000).retrying == []

    assert [%{identifier: ^identifier, cleanup_pending: true}] =
             Orchestrator.snapshot(pid, 1_000).held
  end

  test "stalled remote cleanup failure keeps running state without scheduling a retry" do
    install_fake_ssh!("stalled-cleanup-failure", "exit 17\n")

    {pid, issue, worker_pid} =
      start_budget_orchestrator("stalled-cleanup-failure", 100, 100, 1)

    put_running_entry(pid, issue, worker_pid,
      worker_host: "worker.example",
      workspace_path: "/srv/workspaces/#{issue.identifier}",
      started_at: DateTime.add(DateTime.utc_now(), -1, :second)
    )

    send(pid, :run_poll_cycle)
    state = :sys.get_state(pid)

    assert Map.has_key?(state.running, issue.id)
    assert state.retry_attempts == %{}
    assert Process.alive?(worker_pid)
  end

  test "app-server steering sends the documented live-turn request" do
    test_root = Path.join(System.tmp_dir!(), "symphony-steer-#{System.unique_integer([:positive])}")
    trace = Path.join(test_root, "steer.json")
    File.mkdir_p!(test_root)
    on_exit(fn -> File.rm_rf(test_root) end)

    command = "IFS= read -r line; printf '%s' \"$line\" > '#{trace}'"
    {:ok, port} = ProcessTree.open_port(System.find_executable("sh"), ["-c", command])

    assert :ok = AppServer.steer_turn(port, "thread-1", "turn-1", "checkpoint")
    assert_eventually(fn -> File.exists?(trace) end)

    payload = trace |> File.read!() |> Jason.decode!()
    assert payload["method"] == "turn/steer"
    assert payload["params"]["threadId"] == "thread-1"
    assert payload["params"]["expectedTurnId"] == "turn-1"
    refute Map.has_key?(payload["params"], "turnId")
    assert payload["params"]["input"] == [%{"type" => "text", "text" => "checkpoint"}]
  end

  defp start_budget_orchestrator(
         suffix,
         limit,
         cleanup_timeout_ms \\ 100,
         stall_timeout_ms \\ 0
       ) do
    workspace_root =
      Path.join(System.tmp_dir!(), "symphony-budget-#{suffix}-#{System.unique_integer([:positive])}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root,
      codex_input_token_limit: limit,
      codex_input_token_warning_ratio: 0.70,
      codex_stall_timeout_ms: stall_timeout_ms,
      runner_process_cleanup_timeout_ms: cleanup_timeout_ms
    )

    issue = %Issue{
      id: "issue-#{suffix}",
      identifier: "MT-#{String.upcase(suffix)}",
      title: "Token budget #{suffix}",
      state: "In Progress",
      labels: []
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)

    worker_pid = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(worker_pid), do: Process.exit(worker_pid, :shutdown)
      if Process.alive?(pid), do: Process.exit(pid, :normal)
      File.rm_rf(workspace_root)
    end)

    {pid, issue, worker_pid}
  end

  defp put_running_entry(pid, issue, worker_pid, overrides) do
    started_at = DateTime.utc_now()

    entry = %{
      pid: worker_pid,
      ref: Process.monitor(worker_pid),
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: nil,
      session_id: "thread-turn",
      thread_id: nil,
      turn_id: nil,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_app_server_pid: nil,
      codex_app_server_port: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      input_token_limit: nil,
      input_token_warning_ratio: 0.70,
      input_token_warning_sent: false,
      input_token_warning_status: nil,
      input_token_warning_ack_timer_ref: nil,
      input_token_warning_ack_token: nil,
      input_token_warning_reader_busy: false,
      turn_count: 1,
      retry_attempt: 0,
      started_at: started_at
    }

    entry = Enum.into(overrides, entry)

    :sys.replace_state(pid, fn state ->
      %{
        state
        | running: %{issue.id => entry},
          claimed: MapSet.put(state.claimed, issue.id)
      }
    end)
  end

  defp send_token_update(pid, issue_id, input_tokens) do
    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "codex/event/token_count",
           "params" => %{
             "msg" => %{
               "type" => "token_count",
               "info" => %{
                 "total_token_usage" => %{
                   "input_tokens" => input_tokens,
                   "output_tokens" => 0,
                   "total_tokens" => input_tokens
                 }
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )
  end

  defp send_modern_token_update(pid, issue_id, input_tokens) do
    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "thread/tokenUsage/updated",
           "params" => %{
             "tokenUsage" => %{
               "total" => %{
                 "inputTokens" => input_tokens,
                 "outputTokens" => 0,
                 "totalTokens" => input_tokens
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )
  end

  defp start_replacement_orchestrator do
    {:ok, pid} = Orchestrator.start_link(name: unique_orchestrator_name())

    on_exit(fn ->
      if Process.alive?(pid) do
        try do
          GenServer.stop(pid)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    pid
  end

  defp install_fake_ssh!(suffix, body) do
    test_root =
      Path.join(System.tmp_dir!(), "symphony-fake-ssh-#{suffix}-#{System.unique_integer([:positive])}")

    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")
    File.mkdir_p!(test_root)
    File.write!(fake_ssh, "#!/bin/sh\n" <> body)
    File.chmod!(fake_ssh, 0o755)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    {fake_ssh, test_root}
  end

  defp unique_orchestrator_name do
    Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
  end

  defp os_process_alive?(pid) do
    {_output, status} =
      System.cmd(System.find_executable("kill"), ["-0", Integer.to_string(pid)], stderr_to_stdout: true)

    status == 0
  end

  defp assert_eventually(fun, attempts \\ 40)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")
end
