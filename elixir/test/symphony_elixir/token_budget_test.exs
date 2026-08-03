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

  test "relative persistence root stays anchored to WORKFLOW across a restart from another cwd" do
    original_cwd = File.cwd!()
    workflow_dir = Workflow.workflow_file_path() |> Path.expand() |> Path.dirname()
    relative_root = "relative-workspaces"
    workspace_root = Path.join(workflow_dir, relative_root)
    other_cwd = Path.join(workflow_dir, "restart-cwd")

    hold = %{
      issue_id: "issue-relative-root",
      identifier: "MT-RELATIVE-ROOT",
      reason: "manual_stop",
      limit: nil,
      observed_tokens: 0,
      issue_state: "In Progress",
      held_at: DateTime.utc_now(),
      cleanup_pending: false,
      workspace_path: Path.join(workspace_root, "MT-RELATIVE-ROOT")
    }

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: relative_root,
      codex_input_token_limit: 100
    )

    File.mkdir_p!(other_cwd)
    assert :ok = SymphonyElixir.HoldStore.persist(workspace_root, %{hold.issue_id => hold})

    try do
      File.cd!(workflow_dir)
      first_name = Module.concat(__MODULE__, :RelativeRootFirst)
      {:ok, first_pid} = Orchestrator.start_link(name: first_name)
      assert [%{identifier: "MT-RELATIVE-ROOT"}] = Orchestrator.snapshot(first_name, 1_000).held
      GenServer.stop(first_pid)

      File.cd!(other_cwd)
      restarted_name = Module.concat(__MODULE__, :RelativeRootRestarted)
      {:ok, restarted_pid} = Orchestrator.start_link(name: restarted_name)

      assert [%{identifier: "MT-RELATIVE-ROOT"}] =
               Orchestrator.snapshot(restarted_name, 1_000).held

      runtime_issue = %Issue{
        id: "issue-relative-root-runtime",
        identifier: "MT-RELATIVE-ROOT-RUNTIME",
        title: "Relative persistence root runtime write",
        state: "In Progress",
        labels: []
      }

      worker_pid = spawn(fn -> Process.sleep(:infinity) end)
      put_running_entry(restarted_pid, runtime_issue, worker_pid, input_token_limit: 100)
      send_token_update(restarted_pid, runtime_issue.id, 100)

      assert Enum.any?(
               Orchestrator.snapshot(restarted_name, 1_000).held,
               &(&1.identifier == runtime_issue.identifier)
             )

      runtime_issue_id = runtime_issue.id
      assert {:ok, %{^runtime_issue_id => runtime_hold}} = SymphonyElixir.HoldStore.load(workspace_root)
      assert runtime_hold.reason == "input_token_limit"
      refute Process.alive?(worker_pid)

      GenServer.stop(restarted_pid)
    after
      File.cd!(original_cwd)
    end
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

  test "a worker exit while warning delivery is requested fails closed" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("requested-warning-exit", 100)

    put_running_entry(pid, issue, worker_pid,
      input_token_limit: 100,
      input_token_warning_ratio: 0.70,
      input_token_checkpoint_grace: 10,
      input_token_warning_sent: true,
      input_token_warning_status: "requested",
      input_token_warning_threshold: 70,
      input_token_warning_observed_at: 70
    )

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

  test "a restored bounded resume rejects stale retry authority" do
    {pid, server, issue, worker_pid, _workspace_root} =
      start_budget_orchestrator("resume-restart-authority", 100)

    workspace = Path.join(Config.settings!().workspace.root, issue.identifier)
    put_running_entry(pid, issue, worker_pid, input_token_limit: 100, workspace_path: workspace)
    send_token_update(pid, issue.id, 100)

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [
      %{issue | state: "Todo", labels: ["Symphony Normal"], dispatchable: true}
    ])

    assert {:ok, %{phase: "validation", effective_additional_input_tokens: 50}} =
             Orchestrator.resume_issue(
               issue.identifier,
               %{phase: "validation", max_additional_input_tokens: 50},
               server
             )

    :ok = GenServer.stop(pid)
    {restarted_pid, _restarted_server} = start_replacement_orchestrator()
    restarted_state = :sys.get_state(restarted_pid)

    assert restarted_state.retry_attempts == %{}
    assert restarted_state.holds[issue.id].reason == "input_token_resume_pending"

    task_supervisor = start_supervised!(Task.Supervisor)
    test_pid = self()

    agent_runner = fn _dispatched_issue, _recipient, _opts ->
      send(test_pid, :stale_retry_dispatched)
      Process.sleep(:infinity)
    end

    stale_phase_budget = %{
      phase: "implementation",
      requested_additional_input_tokens: 50,
      effective_additional_input_tokens: 50
    }

    state = %{
      restarted_state
      | task_supervisor: task_supervisor,
        agent_runner: agent_runner,
        max_concurrent_agents: 1
    }

    updated_state =
      Orchestrator.handle_retry_issue_lookup_for_test(
        %{issue | dispatchable: true},
        state,
        issue.id,
        1,
        %{identifier: issue.identifier, phase_budget: stale_phase_budget}
      )

    assert updated_state.running == %{}
    assert updated_state.holds == restarted_state.holds
    refute_receive :stale_retry_dispatched
  end

  test "a stalled bounded resume re-holds without falling through to ordinary dispatch" do
    {pid, _server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("resume-stall", 100)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root,
      codex_input_token_limit: 100,
      codex_stall_timeout_ms: 1_000
    )

    issue = %{issue | dispatchable: true}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    stalled_at = DateTime.add(DateTime.utc_now(), -5, :second)
    workspace = Path.join(workspace_root, issue.identifier)

    pending_hold = %{
      issue_id: issue.id,
      identifier: issue.identifier,
      reason: "input_token_resume_pending",
      limit: 100,
      observed_tokens: 0,
      resume_phase: "review-fix",
      requested_additional_input_tokens: 200,
      effective_additional_input_tokens: 100,
      attempt_input_token_baseline: 0,
      input_token_tier_limit: 100,
      issue_state: issue.state,
      worker_host: nil,
      workspace_path: workspace,
      codex_app_server_pid: nil,
      cleanup_pending: false,
      held_at: DateTime.utc_now()
    }

    assert :ok = SymphonyElixir.HoldStore.persist(workspace_root, %{issue.id => pending_hold})

    put_running_entry(pid, issue, worker_pid,
      workspace_path: workspace,
      resume_phase: "review-fix",
      requested_additional_input_tokens: 200,
      effective_additional_input_tokens: 100,
      input_token_tier_limit: 100,
      input_token_limit: 100,
      last_codex_timestamp: stalled_at,
      started_at: stalled_at
    )

    test_pid = self()

    :sys.replace_state(pid, fn state ->
      %{
        state
        | holds: %{issue.id => pending_hold},
          claimed: MapSet.put(state.claimed, issue.id),
          agent_runner: fn _issue, _recipient, _opts ->
            send(test_pid, :ordinary_dispatch)
            Process.sleep(:infinity)
          end
      }
    end)

    send(pid, :tick)
    Process.sleep(100)

    state = :sys.get_state(pid)
    refute Process.alive?(worker_pid)
    assert state.running == %{}
    assert state.retry_attempts == %{}
    assert state.holds[issue.id].reason == "input_token_checkpoint_failed"
    assert state.holds[issue.id].resume_phase == "review-fix"
    assert MapSet.member?(state.claimed, issue.id)
    refute Orchestrator.should_dispatch_issue_for_test(issue, %{state | claimed: MapSet.new()})
    refute_receive :ordinary_dispatch

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted_hold}} = SymphonyElixir.HoldStore.load(workspace_root)
    assert persisted_hold.reason == "input_token_checkpoint_failed"
    assert persisted_hold.resume_phase == "review-fix"
  end

  test "agent runner leaves a deferred wait model-idle after the completed turn" do
    issue = %Issue{
      id: "issue-deferred-turn",
      identifier: "MT-DEFERRED-TURN",
      title: "Deferred turn",
      state: "In Progress",
      labels: []
    }

    assert :stop =
             AgentRunner.continue_after_turn_for_test(issue,
               continue_after_turn: fn "issue-deferred-turn" -> false end
             )

    assert :continue =
             AgentRunner.continue_after_turn_for_test(issue,
               continue_after_turn: fn "issue-deferred-turn" -> true end
             )

    assert :stop =
             AgentRunner.continue_after_turn_for_test(issue,
               resume_phase: "implementation",
               continue_after_turn: fn _issue_id -> true end
             )
  end

  test "retry dispatch carries the bounded phase budget into the running attempt" do
    workspace_root =
      Path.join(System.tmp_dir!(), "symphony-budget-dispatch-#{System.unique_integer([:positive])}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root,
      codex_input_token_limit: 100
    )

    issue = %Issue{
      id: "issue-resume-dispatch",
      identifier: "MT-RESUME-DISPATCH",
      title: "Bounded resume dispatch",
      state: "In Progress",
      dispatchable: true,
      labels: []
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    {:ok, task_supervisor} = Task.Supervisor.start_link()
    test_pid = self()

    agent_runner = fn dispatched_issue, recipient, opts ->
      send(test_pid, {:agent_runner_opts, dispatched_issue, recipient, opts})
      Process.sleep(:infinity)
    end

    on_exit(fn ->
      if Process.alive?(task_supervisor), do: Process.exit(task_supervisor, :shutdown)
      File.rm_rf(workspace_root)
    end)

    phase_budget = %{
      phase: "validation",
      requested_additional_input_tokens: 200,
      effective_additional_input_tokens: 100,
      attempt_input_token_baseline: 0,
      current_issue_tier_limit: 100
    }

    state =
      %Orchestrator.State{
        task_supervisor: task_supervisor,
        max_concurrent_agents: 1,
        claimed: MapSet.new([issue.id])
      }
      |> Map.put(:agent_runner, agent_runner)

    updated_state =
      Orchestrator.handle_retry_issue_lookup_for_test(issue, state, issue.id, 1, %{
        identifier: issue.identifier,
        phase_budget: phase_budget
      })

    assert %{
             input_token_limit: 100,
             input_token_tier_limit: 100,
             resume_phase: "validation",
             requested_additional_input_tokens: 200,
             effective_additional_input_tokens: 100,
             attempt_input_token_baseline: 0,
             retry_attempt: 1
           } = updated_state.running[issue.id]

    assert_receive {:agent_runner_opts, ^issue, recipient, opts}, 1_000
    assert is_pid(recipient)
    assert opts[:attempt] == 1
    assert opts[:attempt_session_id] == updated_state.running[issue.id].attempt_session_id
    assert opts[:attempt_session_id] =~ ~r/^[A-Za-z0-9_-]{32}$/
    assert opts[:resume_phase] == "validation"
    assert opts[:max_additional_input_tokens] == 100
    assert is_function(opts[:continue_after_turn], 1)
  end

  test "manual stop durably holds a running issue and preserves its workspace" do
    {pid, server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("manual-stop", 100)

    workspace = Path.join(workspace_root, issue.identifier)
    marker = Path.join(workspace, "preserve-manual")
    File.mkdir_p!(workspace)
    File.write!(marker, "kept")
    put_running_entry(pid, issue, worker_pid, workspace_path: workspace)

    assert {:ok, hold} = Orchestrator.stop_issue(issue.identifier, server)
    assert hold.reason == "manual_stop"
    assert hold.cleanup_pending == false
    assert Orchestrator.snapshot(server, 1_000).running == []
    assert Orchestrator.snapshot(server, 1_000).retrying == []
    refute Process.alive?(worker_pid)
    assert File.read!(marker) == "kept"

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted_hold}} = SymphonyElixir.HoldStore.load(workspace_root)
    assert persisted_hold.reason == "manual_stop"
    assert persisted_hold.workspace_path == workspace
  end

  test "manual stop fails closed when the initial hold cannot be persisted" do
    {pid, server, issue, worker_pid, workspace_root} =
      start_budget_orchestrator("manual-stop-persistence-failure", 100)

    state_file = Path.join(workspace_root, ".symphony-holds.json")
    File.mkdir_p!(state_file)
    put_running_entry(pid, issue, worker_pid, [])

    assert {:error, :hold_state_unavailable} = Orchestrator.stop_issue(issue.identifier, server)

    state = :sys.get_state(pid)
    assert state.running == %{}
    assert state.retry_attempts == %{}
    assert state.hold_store_available == false
    assert state.holds[issue.id].reason == "manual_stop"
    assert state.holds[issue.id].cleanup_pending == true
    assert MapSet.member?(state.claimed, issue.id)
    refute Process.alive?(worker_pid)
  end

  test "manual stop durably replaces a scheduled retry with a hold" do
    {pid, server, issue, _worker_pid, workspace_root} =
      start_budget_orchestrator("manual-retry-stop", 100)

    retry_timer = Process.send_after(pid, :unused_retry_timer, 60_000)

    :sys.replace_state(pid, fn state ->
      retry = %{
        identifier: issue.identifier,
        attempt: 2,
        due_at_ms: System.monotonic_time(:millisecond) + 60_000,
        timer_ref: retry_timer,
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

    assert {:ok, hold} = Orchestrator.stop_issue(issue.identifier, server)
    assert hold.reason == "manual_stop"
    assert hold.cleanup_pending == false
    assert Orchestrator.snapshot(server, 1_000).retrying == []
    assert [%{identifier: identifier, reason: "manual_stop"}] = Orchestrator.snapshot(server, 1_000).held
    assert identifier == issue.identifier

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted_hold}} = SymphonyElixir.HoldStore.load(workspace_root)
    assert persisted_hold.workspace_path == "/tmp/#{issue.identifier}"
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
