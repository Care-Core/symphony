defmodule SymphonyElixir.ProgressEfficiencyTest do
  use SymphonyElixir.TestSupport

  @head "0123456789abcdef0123456789abcdef01234567"
  @next_head "fedcba9876543210fedcba9876543210fedcba98"

  test "review authorization reuses an exact-head full review and requires delta after a head change" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("review-reuse")

    assert {:ok, %{changed: true, review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    assert {:error, :stale_review_head} =
             authorize_review(pid, issue, review_hash, "full", observed_local_head: @next_head)

    assert {:ok, %{kind: "full", review_round_count: 0}} =
             authorize_review(pid, issue, review_hash, "full")

    completed_full_review =
      progress_fingerprint()
      |> Map.put(:full_review_verdict, "pass")

    assert {:ok, %{changed: true, review_fingerprint: ^review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(completed_full_review, "full-review-receipt", progress_kind: "review_receipt"),
               pid
             )

    assert {:error, :full_review_already_completed} =
             authorize_review(pid, issue, review_hash, "full")

    changed_head = progress_fingerprint(head: @next_head, diff_checksum: "diff-2")

    assert {:ok, %{review_fingerprint: changed_review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(changed_head, "head-2"),
               pid
             )

    changed_head_options = [
      requested_head: @next_head,
      observed_local_head: @next_head,
      observed_remote_head: @next_head
    ]

    assert {:error, :full_review_already_completed} =
             authorize_review(pid, issue, changed_review_hash, "full", changed_head_options)

    assert {:ok, %{kind: "delta", review_round_count: 1}} =
             authorize_review(pid, issue, changed_review_hash, "delta", changed_head_options)

    completed_delta_review = Map.put(changed_head, :full_review_verdict, "pass")

    assert {:ok, %{changed: true, review_fingerprint: ^changed_review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(completed_delta_review, "delta-review-receipt", progress_kind: "review_receipt"),
               pid
             )

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert persisted["review_fingerprint_hash"] == changed_review_hash
    assert persisted["full_review_count"] == 1
    assert persisted["delta_review_count"] == 1

    Process.exit(worker_pid, :shutdown)
  end

  test "review completion accounting starts only when the authorized receipt is sealed" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("review-receipt-seal")

    fingerprint = progress_fingerprint()

    assert {:ok, %{changed: true, review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    assert {:ok, %{authorized: true, authorization: authorization}} =
             authorize_review(pid, issue, review_hash, "full")

    progress = :sys.get_state(pid).progress[issue.id]
    assert progress["full_review_count"] == 0
    assert progress["review_round_count"] == 0

    issue_id = issue.id
    assert {:ok, %{^issue_id => authorized}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert authorized["full_review_count"] == 0
    assert authorized["review_round_count"] == 0
    assert authorized["last_review_authorization"] == authorization

    completed_fingerprint = Map.put(fingerprint, :full_review_verdict, "pass")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(completed_fingerprint, "review-receipt-1", progress_kind: "review_receipt"),
               pid
             )

    completed = :sys.get_state(pid).progress[issue.id]
    assert completed["full_review_count"] == 1
    assert completed["review_round_count"] == 1
    assert completed["last_completed_review_authorization"] == authorization
    assert completed["last_review_receipt"] == "review-receipt-1"

    assert {:ok, %{changed: false}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(completed_fingerprint, "review-receipt-1", progress_kind: "review_receipt"),
               pid
             )

    assert :sys.get_state(pid).progress[issue.id]["full_review_count"] == 1
    assert {:error, :full_review_already_completed} = authorize_review(pid, issue, review_hash, "full")

    Process.exit(worker_pid, :shutdown)
  end

  test "a deferred watcher stays model-idle and wakes exactly once on terminal provider truth" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("deferred-watcher")

    assert {:ok, %{review_fingerprint: _review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_state: "waiting", watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    refute GenServer.call(pid, {:continue_after_turn, issue.id})
    send(worker_pid, :finish)

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      state.running == %{} and map_size(state.deferred) == 1 and state.retry_attempts == %{}
    end)

    write_receipt(receipt_path, %{
      "type" => "provider",
      "poll" => 1,
      "observedAt" => "2026-07-24T00:00:00Z"
    })

    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      progress = state.progress[issue.id]

      state.retry_attempts == %{} and
        get_in(progress, ["watcher", "state"]) == "waiting" and
        get_in(progress, ["fingerprint", "hosted_receipt", "state"]) ==
          "provider_transition"
    end)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      watcher = get_in(state.progress, [issue.id, "watcher"])

      state.deferred == %{} and
        map_size(state.retry_attempts) == 1 and
        watcher["state"] == "passed" and
        watcher["wake_count"] == 1
    end)

    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})
    Process.sleep(25)

    state = :sys.get_state(pid)
    assert map_size(state.retry_attempts) == 1
    assert get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
  end

  test "a waiting watcher survives restart and terminal truth wakes one continuation" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-restart")

    assert {:ok, %{review_fingerprint: _review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/restart-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      Map.has_key?(state.deferred, issue.id) and
        MapSet.member?(state.claimed, issue.id) and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "waiting"
    end)

    write_receipt(receipt_path, terminal_receipt("failed"))
    send(restarted_pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      state.deferred == %{} and
        map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "failed" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
  end

  test "a restored phase-bounded wait wakes with its durable resume authority" do
    {pid, issue, worker_pid, workspace_root, workspace} =
      start_progress_orchestrator("phase-watcher-restart")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    phase_budget = %{
      phase: "review-fix",
      requested_additional_input_tokens: 600,
      effective_additional_input_tokens: 600
    }

    put_phase_resume_state(pid, issue, workspace_root, workspace, phase_budget)

    receipt_path = Path.join(workspace, "output/phase-restart-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      Map.has_key?(state.deferred, issue.id) and
        get_in(state.holds, [issue.id, :reason]) == "input_token_resume_pending"
    end)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(restarted_pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      state.deferred == %{} and
        get_in(state.retry_attempts, [issue.id, :phase_budget]) == phase_budget and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
  end

  test "a warning-delivered session enters deferred waiting and wakes once" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("warning-watcher")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    :sys.replace_state(pid, fn state ->
      running_entry =
        state.running
        |> Map.fetch!(issue.id)
        |> Map.put(:input_token_warning_status, "delivered")

      %{state | running: Map.put(state.running, issue.id, running_entry)}
    end)

    receipt_path = Path.join(workspace, "output/warning-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      Map.has_key?(state.deferred, issue.id) and not Map.has_key?(state.holds, issue.id)
    end)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      state.deferred == %{} and map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
  end

  test "stopping a deferred issue cancels its waiter and creates a durable hold" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("stop-deferred")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/stop-checks.jsonl")

    assert {:ok, %{watcher_state: "waiting"}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    old_waiter_pid = waiter_task_pid(:sys.get_state(pid), issue.id)
    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    assert {:ok, %{reason: "manual_stop"}} = Orchestrator.stop_issue(issue.identifier, pid)

    state = :sys.get_state(pid)
    assert state.deferred == %{}
    assert state.waiter_tasks == %{}
    assert get_in(state.progress, [issue.id, "watcher", "state"]) == "cancelled"
    assert get_in(state.holds, [issue.id, :reason]) == "manual_stop"
    refute Process.alive?(old_waiter_pid)
  end

  test "a changed PR head wakes once without accepting the stale head as passed" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-head-change")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/head-change-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)
    assert_eventually(fn -> Map.has_key?(:sys.get_state(pid).deferred, issue.id) end)

    write_receipt(receipt_path, %{
      "type" => "terminal",
      "status" => "head_changed",
      "expectedHead" => @head,
      "observedHead" => @next_head,
      "polls" => 2
    })

    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "head_changed" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1 and
        get_in(state.progress, [issue.id, "fingerprint", "hosted_receipt", "state"]) ==
          "head_changed"
    end)
  end

  test "a deferred watcher timeout wakes one fail-closed continuation" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-timeout")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/timeout-checks.jsonl")

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)
    assert_eventually(fn -> Map.has_key?(:sys.get_state(pid).deferred, issue.id) end)

    :sys.replace_state(pid, fn state ->
      progress =
        update_in(state.progress, [issue.id, "watcher"], fn watcher ->
          Map.put(watcher, "deadline_unix_ms", System.system_time(:millisecond) - 1)
        end)

      %{state | progress: progress}
    end)

    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "timed_out" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1 and
        get_in(state.progress, [issue.id, "fingerprint", "hosted_receipt", "state"]) ==
          "timed_out"
    end)
  end

  test "a waiter exit without a terminal receipt fails closed and wakes one diagnostic continuation" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-missing-terminal")

    waiter_script = Path.join(workspace, "waiter-fixture.sh")
    File.write!(waiter_script, "#!/bin/sh\nexit 17\n")
    File.chmod!(waiter_script, 0o700)

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/missing-terminal-checks.jsonl")

    assert {:ok, %{watcher_state: "waiting"}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "receipt_invalid" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1 and
        state.waiter_tasks == %{}
    end)
  end

  test "a task-supervisor launch failure settles the durable watcher immediately" do
    {pid, issue, _worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("waiter-launch-failure")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    :sys.replace_state(pid, fn state ->
      %{state | task_supervisor: Module.concat(__MODULE__, :MissingTaskSupervisor)}
    end)

    receipt_path = Path.join(workspace, "output/launch-failure-checks.jsonl")

    assert {:error, :waiter_launch_failed} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    assert Process.alive?(pid)
    state = :sys.get_state(pid)
    assert state.waiter_tasks == %{}
    assert get_in(state.progress, [issue.id, "watcher", "state"]) == "receipt_invalid"
    assert get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
  end

  test "a waiter command startup exception settles the durable watcher immediately" do
    {pid, issue, _worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("waiter-command-failure")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    waiter_script = Path.join(workspace, "broken-waiter-fixture.sh")
    File.write!(waiter_script, "#!/definitely/missing\n")
    File.chmod!(waiter_script, 0o600)
    receipt_path = Path.join(workspace, "output/command-failure-checks.jsonl")

    attributes =
      workspace
      |> deferred_wait_attributes(receipt_path)
      |> Map.put(:waiter_script, waiter_script)

    assert {:ok, %{watcher_state: "waiting"}} =
             Orchestrator.register_deferred_wait(issue.identifier, attributes, pid)

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      state.waiter_tasks == %{} and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "receipt_invalid" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
  end

  test "re-registering a wait stops the old waiter and an old exit cannot delete the replacement" do
    {pid, issue, _worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("replace-waiter")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    first_receipt_path = Path.join(workspace, "output/first-checks.jsonl")

    assert {:ok, %{watcher_token: first_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, first_receipt_path),
               pid
             )

    first_waiter_pid = waiter_task_pid(:sys.get_state(pid), issue.id)
    second_receipt_path = Path.join(workspace, "output/second-checks.jsonl")

    assert {:ok, %{watcher_token: second_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, second_receipt_path),
               pid
             )

    refute first_token == second_token
    refute Process.alive?(first_waiter_pid)
    second_waiter_pid = waiter_task_pid(:sys.get_state(pid), issue.id)
    assert Process.alive?(second_waiter_pid)

    send(pid, {:deferred_waiter_exit, issue.id, first_token, :launch_failed})
    Process.sleep(25)

    state = :sys.get_state(pid)
    assert waiter_task_pid(state, issue.id) == second_waiter_pid
    assert get_in(state.progress, [issue.id, "watcher", "token"]) == second_token
    assert get_in(state.progress, [issue.id, "watcher", "state"]) == "waiting"
  end

  test "a suppressed terminal wake still removes stale deferred metadata" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("suppressed-wake")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/suppressed-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    :sys.replace_state(pid, fn state ->
      hold = %{
        issue_id: issue.id,
        identifier: issue.identifier,
        reason: "manual_stop",
        cleanup_pending: false
      }

      %{state | holds: Map.put(state.holds, issue.id, hold)}
    end)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      state.deferred == %{} and state.retry_attempts == %{}
    end)
  end

  test "terminal watcher persistence failure never launches a continuation" do
    {pid, issue, worker_pid, workspace_root, workspace} =
      start_progress_orchestrator("watcher-persist-failure")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/persist-failure-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    progress_path = Path.join(workspace_root, ".symphony-progress.json")
    File.rm!(progress_path)
    File.mkdir_p!(progress_path)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      state.progress_state_available == false and
        state.retry_attempts == %{} and
        Map.has_key?(state.deferred, issue.id) and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "waiting"
    end)
  end

  test "progress persistence failure rejects transitions and safely holds the running worker" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("persistence-failure")

    progress_path = Path.join(workspace_root, ".symphony-progress.json")
    File.mkdir_p!(progress_path)

    assert {:error, :progress_state_unavailable} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    assert_eventually(fn ->
      get_in(:sys.get_state(pid).holds, [issue.id, :identifier]) == issue.identifier and
        get_in(:sys.get_state(pid).holds, [issue.id, :reason]) == "progress_state_unavailable"
    end)

    refute Process.alive?(worker_pid)
    assert :sys.get_state(pid).running == %{}

    assert {:error, :progress_state_unavailable} =
             Orchestrator.resume_issue(
               issue.identifier,
               %{phase: "implementation", max_additional_input_tokens: 100},
               pid
             )

    assert {:error, :progress_state_unavailable} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-2"),
               pid
             )
  end

  defp start_progress_orchestrator(suffix) do
    workspace_root =
      Path.join(System.tmp_dir!(), "symphony-progress-#{suffix}-#{System.unique_integer([:positive])}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root
    )

    issue = %Issue{
      id: "issue-#{suffix}",
      identifier: "MT-#{String.upcase(suffix)}",
      title: "Progress efficiency #{suffix}",
      state: "In Progress",
      labels: []
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)
    worker_pid = spawn(fn -> wait_for_finish() end)
    workspace = Path.join(workspace_root, issue.identifier)
    File.mkdir_p!(workspace)
    install_waiter_fixture!(workspace)

    entry = %{
      pid: worker_pid,
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: workspace,
      session_id: "thread-turn",
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      turn_count: 1,
      retry_attempt: 0,
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn state ->
      monitored_entry = Map.put(entry, :ref, Process.monitor(worker_pid))

      %{
        state
        | running: %{issue.id => monitored_entry},
          claimed: MapSet.put(state.claimed, issue.id)
      }
    end)

    on_exit(fn ->
      if Process.alive?(worker_pid), do: Process.exit(worker_pid, :shutdown)
      if Process.alive?(pid), do: Process.exit(pid, :normal)
      File.rm_rf(workspace_root)
    end)

    {pid, issue, worker_pid, workspace_root, workspace}
  end

  defp deferred_wait_attributes(workspace, receipt_path) do
    %{
      expected_head: @head,
      receipt_path: receipt_path,
      timeout_seconds: 1_200,
      waiter_script: Path.join(workspace, "waiter-fixture.sh"),
      waiter_args: [
        "--collect-terminal",
        "--head",
        @head,
        "--output",
        receipt_path
      ]
    }
  end

  defp start_replacement_orchestrator do
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    pid
  end

  defp put_phase_resume_state(pid, issue, workspace_root, workspace, phase_budget) do
    hold = %{
      issue_id: issue.id,
      identifier: issue.identifier,
      reason: "input_token_resume_pending",
      limit: phase_budget.effective_additional_input_tokens,
      observed_tokens: 0,
      issue_state: issue.state,
      worker_host: nil,
      workspace_path: workspace,
      codex_app_server_pid: nil,
      cleanup_pending: false,
      held_at: DateTime.utc_now(),
      resume_phase: phase_budget.phase,
      requested_additional_input_tokens: phase_budget.requested_additional_input_tokens,
      effective_additional_input_tokens: phase_budget.effective_additional_input_tokens
    }

    :sys.replace_state(pid, fn state ->
      running_entry =
        state.running
        |> Map.fetch!(issue.id)
        |> Map.merge(%{
          resume_phase: phase_budget.phase,
          requested_additional_input_tokens: phase_budget.requested_additional_input_tokens,
          effective_additional_input_tokens: phase_budget.effective_additional_input_tokens
        })

      %{
        state
        | running: Map.put(state.running, issue.id, running_entry),
          holds: Map.put(state.holds, issue.id, hold)
      }
    end)

    assert :ok = SymphonyElixir.HoldStore.persist(workspace_root, %{issue.id => hold})
  end

  defp waiter_task_pid(state, issue_id) do
    case Map.fetch!(state.waiter_tasks, issue_id) do
      pid when is_pid(pid) -> pid
      %{pid: pid} when is_pid(pid) -> pid
    end
  end

  defp install_waiter_fixture!(workspace) do
    script = Path.join(workspace, "waiter-fixture.sh")
    File.write!(script, "#!/bin/sh\nwhile :; do sleep 60; done\n")
    File.chmod!(script, 0o700)
  end

  defp progress_fingerprint(overrides \\ []) do
    head = Keyword.get(overrides, :head, @head)

    %{
      contract_revision: "SYMPHONY-UPSTREAM-REBASE-v1",
      base_sha: @head,
      head_sha: head,
      diff_checksum: Keyword.get(overrides, :diff_checksum, "diff-1"),
      matrix_checksum: "matrix-v2",
      required_check_set: ["Build Check", "Test & Lint"],
      latest_human_comment_checkpoint: nil,
      full_review_verdict: nil,
      hosted_receipt: nil
    }
  end

  defp progress_attributes(fingerprint, receipt, overrides \\ []) do
    %{
      fingerprint: fingerprint,
      progress_kind: Keyword.get(overrides, :progress_kind, "workpad_checkpoint"),
      progress_receipt: receipt
    }
  end

  defp authorize_review(pid, issue, review_hash, kind, overrides \\ []) do
    requested_head = Keyword.get(overrides, :requested_head, @head)

    Orchestrator.authorize_review(
      issue.identifier,
      %{
        kind: kind,
        review_fingerprint: review_hash,
        requested_head: requested_head,
        observed_local_head: Keyword.get(overrides, :observed_local_head, requested_head),
        observed_remote_head: Keyword.get(overrides, :observed_remote_head, requested_head),
        human_override: Keyword.get(overrides, :human_override)
      },
      pid
    )
  end

  defp terminal_receipt(status) do
    %{
      "type" => "terminal",
      "status" => status,
      "expectedHead" => @head,
      "observedHead" => @head,
      "polls" => 2
    }
  end

  defp write_receipt(path, receipt) do
    File.write!(path, Jason.encode!(receipt) <> "\n")
  end

  defp wait_for_finish do
    receive do
      :finish -> :ok
    end
  end

  defp assert_eventually(fun, attempts \\ 80)

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
