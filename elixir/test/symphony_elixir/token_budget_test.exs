defmodule SymphonyElixir.TokenBudgetTest do
  use SymphonyElixir.TestSupport

  test "config selects the smallest matching case-insensitive label limit" do
    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: 1_000,
      codex_input_token_limits_by_label: %{"Symphony Normal" => 800, "URGENT" => 250}
    )

    issue = %Issue{labels: [" symphony normal ", "urgent"]}
    assert Config.input_token_limit_for_issue(issue) == 250
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == 1_000

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: nil,
      codex_input_token_limits_by_label: %{"Symphony Normal" => 400}
    )

    assert Config.input_token_limit_for_issue(%Issue{labels: ["SYMPHONY NORMAL"]}) == 400
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == nil
  end

  test "config rejects non-positive limits and invalid warning or grace values" do
    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_limit: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limit"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{"symphony normal" => -1}
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limits_by_label"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{" " => 100}
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "labels must not be blank"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{"Symphony Normal" => 800, " symphony normal " => 400}
    )

    assert :ok = Config.validate!()
    assert Config.input_token_limit_for_issue(%Issue{labels: ["SYMPHONY NORMAL"]}) == 400

    for limits <- [
          %{"Symphony Normal" => -1, " symphony normal " => 400},
          %{"Symphony Normal" => 400, " symphony normal " => -1}
        ] do
      write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_limits_by_label: limits)
      assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
      assert message =~ "limits must be positive integers"
    end

    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_warning_ratio: 1.0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_warning_ratio"

    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_checkpoint_grace: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_checkpoint_grace"
  end

  test "exact hard limit stops the worker, suppresses retries, and persists a hold" do
    {pid, server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("hard-limit", 100)

    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)

    send_token_update(pid, issue.id, 100)

    snapshot = Orchestrator.snapshot(server, 5_000)
    assert snapshot.running == []
    assert snapshot.retrying == []
    assert [%{reason: "input_token_limit", limit: 100, observed_tokens: 100}] = snapshot.held
    refute Process.alive?(worker_pid)

    issue_id = issue.id
    assert {:ok, %{^issue_id => hold}} = SymphonyElixir.HoldStore.load(workspace_root)
    assert hold.reason == "input_token_limit"
    assert hold.observed_tokens == 100
  end

  test "hard-limit enforcement stops even when hold persistence is unavailable" do
    {pid, _server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("hard-limit-hold-retry", 100)

    state_file = Path.join(workspace_root, ".symphony-holds.json")
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

  test "budget holds survive orchestrator restart" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("restart", 100)

    put_running_entry(pid, issue, worker_pid, input_token_limit: 100)
    send_token_update(pid, issue.id, 100)
    assert [%{identifier: identifier}] = Orchestrator.snapshot(server, 1_000).held
    assert identifier == issue.identifier

    :ok = GenServer.stop(pid)
    {_restarted_pid, restarted_server} = start_replacement_orchestrator()

    assert [%{identifier: ^identifier, reason: "input_token_limit"}] =
             Orchestrator.snapshot(restarted_server, 1_000).held

    assert MapSet.member?(:sys.get_state(restarted_server).claimed, issue.id)
  end

  test "warning threshold fails closed when the live steering channel is unavailable" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("warning-unsupported", 100)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      input_token_warning_ratio: 0.70,
      input_token_checkpoint_grace: 10,
      input_token_warning_sent: false,
      input_token_warning_status: nil
    )

    send_token_update(pid, issue.id, 69)
    assert [_running] = Orchestrator.snapshot(server, 1_000).running

    send_token_update(pid, issue.id, 70)
    snapshot = Orchestrator.snapshot(server, 1_000)

    assert snapshot.running == []
    assert snapshot.retrying == []

    assert [%{reason: "input_token_warning_unsupported", limit: 100, observed_tokens: 70}] =
             snapshot.held

    refute Process.alive?(worker_pid)
  end

  test "delivered warning is held when checkpoint grace is exhausted" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("checkpoint-grace", 100)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      input_token_warning_ratio: 0.70,
      input_token_checkpoint_grace: 10,
      input_token_warning_sent: true,
      input_token_warning_status: "delivered",
      input_token_warning_threshold: 70,
      input_token_warning_observed_at: 70
    )

    send_token_update(pid, issue.id, 80)
    snapshot = Orchestrator.snapshot(server, 1_000)

    assert snapshot.running == []
    assert snapshot.retrying == []

    assert [%{reason: "input_token_checkpoint_grace", limit: 100, observed_tokens: 80}] =
             snapshot.held

    refute Process.alive?(worker_pid)
  end

  test "a delivered warning permits the checkpoint turn to finish and then holds" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("delivered-warning", 100)

    executable = System.find_executable("cat")
    codex_port = Port.open({:spawn_executable, executable}, [:binary, :exit_status])

    on_exit(fn ->
      if :erlang.port_info(codex_port) != :undefined, do: Port.close(codex_port)
    end)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      input_token_warning_ratio: 0.70,
      input_token_checkpoint_grace: 10,
      input_token_warning_sent: false,
      input_token_warning_status: nil
    )

    send(
      pid,
      {:codex_worker_update, issue.id,
       %{
         event: :session_started,
         session_id: "thread-warning-turn-warning",
         codex_app_server_port: codex_port,
         thread_id: "thread-warning",
         turn_id: "turn-warning",
         timestamp: DateTime.utc_now()
       }}
    )

    send_token_update(pid, issue.id, 70)

    assert [%{input_token_warning_status: "requested"}] =
             Orchestrator.snapshot(server, 1_000).running

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
             Orchestrator.snapshot(server, 1_000).running

    running_ref = :sys.get_state(pid).running[issue.id].ref
    send(pid, {:DOWN, running_ref, :process, worker_pid, :normal})

    assert [%{reason: "input_token_checkpoint", warning_threshold: 70}] =
             Orchestrator.snapshot(server, 1_000).held

    assert Orchestrator.snapshot(server, 1_000).retrying == []
  end

  test "a bounded resumed attempt re-holds on every worker exit" do
    for {suffix, exit_reason, expected_hold_reason} <- [
          {"resume-normal-exit", :normal, "input_token_checkpoint"},
          {"resume-failed-exit", :boom, "input_token_checkpoint_failed"}
        ] do
      {pid, server, issue, worker_pid, _workspace_root} =
        start_budget_orchestrator(suffix, 100)

      put_running_entry(pid, issue, worker_pid,
        input_token_limit: 50,
        input_token_tier_limit: 100,
        resume_phase: "review-fix",
        requested_additional_input_tokens: 50,
        effective_additional_input_tokens: 50,
        attempt_input_token_baseline: 0
      )

      running_ref = :sys.get_state(pid).running[issue.id].ref
      send(pid, {:DOWN, running_ref, :process, worker_pid, exit_reason})

      snapshot = Orchestrator.snapshot(server, 1_000)
      assert snapshot.retrying == []

      assert [
               %{
                 reason: ^expected_hold_reason,
                 resume_phase: "review-fix",
                 effective_additional_input_tokens: 50
               }
             ] = snapshot.held
    end
  end

  test "explicit resume durably authorizes one bounded continuation despite tracker state changes" do
    {pid, server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("resume", 100)

    workspace = Path.join(workspace_root, issue.identifier)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100, workspace_path: workspace)
    send_token_update(pid, issue.id, 100)

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [
      %{issue | state: "Todo", labels: ["Symphony Normal"]}
    ])

    assert {:error, :resume_phase_required} = Orchestrator.resume_issue(issue.identifier, server)

    assert {:error, :invalid_resume_phase} =
             Orchestrator.resume_issue(
               issue.identifier,
               %{phase: "unknown", max_additional_input_tokens: 50},
               server
             )

    assert {:error, :max_additional_input_tokens_required} =
             Orchestrator.resume_issue(issue.identifier, %{phase: "validation"}, server)

    assert {:ok,
            %{
              resumed: true,
              phase: "validation",
              requested_additional_input_tokens: 200,
              effective_additional_input_tokens: 100,
              attempt_input_token_baseline: 0
            }} =
             Orchestrator.resume_issue(
               issue.identifier,
               %{phase: " Validation ", max_additional_input_tokens: 200},
               server
             )

    assert [
             %{
               reason: "input_token_resume_pending",
               resume_phase: "validation",
               requested_additional_input_tokens: 200,
               effective_additional_input_tokens: 100,
               attempt_input_token_baseline: 0,
               input_token_tier_limit: 100,
               workspace_path: ^workspace
             }
           ] = Orchestrator.snapshot(server, 1_000).held

    retry = :sys.get_state(pid).retry_attempts[issue.id]
    assert retry.attempt == 1
    assert retry.identifier == issue.identifier
    assert retry.workspace_path == workspace

    assert retry.phase_budget == %{
             phase: "validation",
             requested_additional_input_tokens: 200,
             effective_additional_input_tokens: 100,
             attempt_input_token_baseline: 0,
             current_issue_tier_limit: 100
           }

    assert MapSet.member?(:sys.get_state(pid).claimed, issue.id)
    refute Orchestrator.should_dispatch_issue_for_test(issue, :sys.get_state(pid))
  end

  defp start_budget_orchestrator(suffix, limit) do
    workspace_root =
      Path.join(System.tmp_dir!(), "symphony-budget-#{suffix}-#{System.unique_integer([:positive])}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root,
      codex_input_token_limit: limit,
      codex_input_token_warning_ratio: 0.70
    )

    issue = %Issue{
      id: "issue-#{suffix}",
      identifier: "MT-#{String.upcase(suffix)}",
      title: "Token budget #{suffix}",
      state: "In Progress",
      labels: []
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)
    worker_pid = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(worker_pid), do: Process.exit(worker_pid, :shutdown)
      if Process.alive?(pid), do: Process.exit(pid, :normal)
      File.rm_rf(workspace_root)
    end)

    {pid, name, issue, worker_pid, workspace_root}
  end

  defp put_running_entry(pid, issue, worker_pid, overrides) do
    entry =
      Enum.into(overrides, %{
        pid: worker_pid,
        ref: Process.monitor(worker_pid),
        identifier: issue.identifier,
        issue: issue,
        worker_host: nil,
        workspace_path: nil,
        session_id: "thread-turn",
        last_codex_message: nil,
        last_codex_timestamp: nil,
        last_codex_event: nil,
        codex_app_server_pid: nil,
        codex_input_tokens: 0,
        codex_output_tokens: 0,
        codex_total_tokens: 0,
        codex_last_reported_input_tokens: 0,
        codex_last_reported_output_tokens: 0,
        codex_last_reported_total_tokens: 0,
        turn_count: 1,
        retry_attempt: 0,
        started_at: DateTime.utc_now()
      })

    :sys.replace_state(pid, fn state ->
      %{state | running: %{issue.id => entry}, retry_attempts: %{}, claimed: MapSet.put(state.claimed, issue.id)}
    end)
  end

  defp start_replacement_orchestrator do
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :normal)
    end)

    {pid, name}
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
end
