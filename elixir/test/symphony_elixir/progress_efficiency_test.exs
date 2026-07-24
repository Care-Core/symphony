defmodule SymphonyElixir.ProgressEfficiencyTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.HoldStore

  @head "0123456789abcdef0123456789abcdef01234567"
  @next_head "fedcba9876543210fedcba9876543210fedcba98"

  test "progress fingerprints persist and unchanged receipts preserve accumulated counters" do
    {pid, issue, _worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("persisted-progress",
        codex_no_progress_input_tokens: 1_000,
        codex_no_progress_cycles: 10
      )

    fingerprint = progress_fingerprint()

    assert {:ok,
            %{
              changed: true,
              progress_fingerprint: progress_hash,
              review_fingerprint: review_hash
            }} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 250)
    send_session_started(pid, issue.id, "thread-turn-1")

    assert_eventually(fn ->
      case Orchestrator.snapshot(pid, 1_000).running do
        [%{tokens_since_progress: 250, model_cycles_since_progress: 1}] -> true
        _ -> false
      end
    end)

    assert {:ok,
            %{
              changed: false,
              progress_fingerprint: ^progress_hash,
              review_fingerprint: ^review_hash,
              tokens_since_progress: 250,
              model_cycles_since_progress: 1
            }} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = HoldStore.load_progress(workspace_root)
    assert persisted["progress_fingerprint_hash"] == progress_hash
    assert persisted["review_fingerprint_hash"] == review_hash
    assert persisted["tokens_since_progress"] == 250
    assert persisted["model_cycles_since_progress"] == 1

    GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert {:ok,
            %{
              changed: false,
              progress_fingerprint: ^progress_hash,
              review_fingerprint: ^review_hash,
              tokens_since_progress: 250,
              model_cycles_since_progress: 1
            }} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               restarted_pid
             )
  end

  test "review authorization rejects stale heads and enforces one full plus one delta per fingerprint" do
    {pid, issue, _worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("review-rounds")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    assert {:error, :stale_review_head} =
             authorize_review(pid, issue, review_hash, "full", observed_local_head: @next_head)

    assert {:ok, %{kind: "full", review_round_count: 1}} =
             authorize_review(pid, issue, review_hash, "full")

    provider_only_fingerprint =
      Map.put(progress_fingerprint(), :hosted_receipt, %{"status" => "passed", "polls" => 2})

    assert {:ok, %{review_fingerprint: ^review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(provider_only_fingerprint, "provider-terminal", progress_kind: "provider_transition"),
               pid
             )

    assert {:error, :full_review_already_completed} =
             authorize_review(pid, issue, review_hash, "full")

    review_outcome_fingerprint =
      Map.put(provider_only_fingerprint, :full_review_verdict, "pass")

    assert {:ok, %{review_fingerprint: ^review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(review_outcome_fingerprint, "full-review-pass", progress_kind: "review_receipt"),
               pid
             )

    assert {:error, :full_review_already_completed} =
             authorize_review(pid, issue, review_hash, "full")

    assert {:ok, %{kind: "delta", review_round_count: 2}} =
             authorize_review(pid, issue, review_hash, "delta")

    assert {:error, :delta_review_already_completed} =
             authorize_review(pid, issue, review_hash, "delta")

    human_override =
      "linear-comment:12345678-1234-1234-1234-123456789abc@2026-07-24T12:34:56.000Z"

    assert {:ok, %{kind: "delta", review_round_count: 3}} =
             authorize_review(pid, issue, review_hash, "delta", human_override: human_override)

    assert {:error, :review_override_already_used} =
             authorize_review(pid, issue, review_hash, "delta", human_override: human_override)

    assert {:ok, %{kind: "security", security_review_count: 1}} =
             authorize_review(pid, issue, review_hash, "security")

    assert {:error, :security_review_already_completed} =
             authorize_review(pid, issue, review_hash, "security")

    changed_head_fingerprint = progress_fingerprint(head: @next_head, diff_checksum: "diff-2")

    assert {:ok, %{review_fingerprint: changed_review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(changed_head_fingerprint, "head-2"),
               pid
             )

    assert {:error, :full_review_already_completed} =
             authorize_review(pid, issue, changed_review_hash, "full",
               requested_head: @next_head,
               observed_local_head: @next_head,
               observed_remote_head: @next_head
             )

    assert {:ok, %{kind: "delta", review_round_count: 2}} =
             authorize_review(pid, issue, changed_review_hash, "delta",
               requested_head: @next_head,
               observed_local_head: @next_head,
               observed_remote_head: @next_head
             )

    revised_contract =
      progress_fingerprint(
        head: @next_head,
        diff_checksum: "diff-3",
        contract_revision: "SYMPHONY-PROGRESS-EFFICIENCY-v3",
        matrix_checksum: "matrix-v3"
      )

    assert {:ok, %{review_fingerprint: revised_review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(revised_contract, "contract-v3"),
               pid
             )

    assert {:ok, %{kind: "full", review_round_count: 1}} =
             authorize_review(pid, issue, revised_review_hash, "full",
               requested_head: @next_head,
               observed_local_head: @next_head,
               observed_remote_head: @next_head
             )
  end

  test "review receipt acceptance is bound to the authorized exact head" do
    {pid, issue, _worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("review-receipt-head")

    authorized_fingerprint = progress_fingerprint(head: @next_head, diff_checksum: "diff-2")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(authorized_fingerprint, "head-2"),
               pid
             )

    assert {:ok, %{kind: "full"}} =
             authorize_review(pid, issue, review_hash, "full",
               requested_head: @next_head,
               observed_local_head: @next_head,
               observed_remote_head: @next_head
             )

    stale_receipt_fingerprint =
      progress_fingerprint()
      |> Map.put(:full_review_verdict, "pass")

    assert {:error, :review_authorization_mismatch} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(stale_receipt_fingerprint, "review-head-a", progress_kind: "review_receipt"),
               pid
             )
  end

  test "meaningful progress resets the token breaker and unchanged growth creates a durable hold" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("token-breaker",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 100,
        codex_no_progress_cycles: 10
      )

    fingerprint = progress_fingerprint()

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 99)

    assert_eventually(fn ->
      match?(
        [%{tokens_since_progress: 99}],
        Orchestrator.snapshot(pid, 1_000).running
      )
    end)

    validation_fingerprint =
      Map.put(fingerprint, :hosted_receipt, %{
        "state" => "validation_complete",
        "receipt" => "validation-receipt-1"
      })

    assert {:ok, %{changed: true, tokens_since_progress: 0}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(validation_fingerprint, "validation-receipt-1", progress_kind: "validation_receipt"),
               pid
             )

    send_token_update(pid, issue.id, 198)

    assert_eventually(fn ->
      match?(
        [%{tokens_since_progress: 99}],
        Orchestrator.snapshot(pid, 1_000).running
      )
    end)

    send_token_update(pid, issue.id, 199)

    assert_eventually(fn ->
      match?(
        [%{reason: "no_progress", limit: 100, observed_tokens: 199}],
        Orchestrator.snapshot(pid, 1_000).held
      )
    end)

    refute Process.alive?(worker_pid)
  end

  test "changed receipt text without a changed fingerprint does not reset the breaker" do
    {pid, issue, _worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("receipt-bytes",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 100,
        codex_no_progress_cycles: 10
      )

    fingerprint = progress_fingerprint()

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 50)

    assert_eventually(fn ->
      match?(
        [%{tokens_since_progress: 50}],
        Orchestrator.snapshot(pid, 1_000).running
      )
    end)

    assert {:ok, %{changed: false, tokens_since_progress: 50}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "novel-free-form-receipt"),
               pid
             )
  end

  test "no-progress tokens accumulate across attempts" do
    {pid, issue, _worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("cross-attempt-breaker",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 100,
        codex_no_progress_cycles: 10
      )

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 60)

    assert_eventually(fn ->
      match?(
        [%{tokens_since_progress: 60}],
        Orchestrator.snapshot(pid, 1_000).running
      )
    end)

    begin_new_attempt(pid, issue.id)
    send_token_update(pid, issue.id, 40)

    assert_eventually(fn ->
      match?(
        [%{reason: "no_progress", limit: 100}],
        Orchestrator.snapshot(pid, 1_000).held
      )
    end)
  end

  test "completed model cycles create the same no-progress hold without token growth" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("cycle-breaker",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 1_000,
        codex_no_progress_cycles: 2
      )

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    send_session_started(pid, issue.id, "thread-turn-1")

    assert_eventually(fn ->
      match?(
        [%{model_cycles_since_progress: 1}],
        Orchestrator.snapshot(pid, 1_000).running
      )
    end)

    send_session_started(pid, issue.id, "thread-turn-2")

    assert_eventually(fn ->
      match?(
        [%{reason: "no_progress"}],
        Orchestrator.snapshot(pid, 1_000).held
      )
    end)

    refute Process.alive?(worker_pid)
  end

  test "the orchestrator-owned waiter collects terminal truth without another model cycle" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("owned-waiter")

    waiter_script = Path.join(workspace, "waiter-fixture.sh")

    File.write!(
      waiter_script,
      """
      #!/bin/sh
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --head) head="$2"; shift 2 ;;
          --output) output="$2"; shift 2 ;;
          *) shift ;;
        esac
      done
      mkdir -p "$(dirname "$output")"
      printf '%s\n' '{"type":"provider","poll":1,"observedAt":"2026-07-24T00:00:00Z"}' >> "$output"
      sleep 0.2
      printf '{"type":"terminal","status":"passed","expectedHead":"%s","observedHead":"%s","polls":1}\n' "$head" "$head" >> "$output"
      """
    )

    File.chmod!(waiter_script, 0o700)

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/owned-checks.jsonl")

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
        state.running == %{} and
        state.deferred == %{} and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "passed" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1 and
        get_in(state.progress, [issue.id, "model_cycles_since_progress"]) == 0
    end)
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
      snapshot = Orchestrator.snapshot(pid, 1_000)
      snapshot.running == [] and length(snapshot.deferred) == 1 and snapshot.retrying == []
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
          "provider_transition" and
        progress["model_cycles_since_progress"] == 0
    end)

    write_receipt(receipt_path, %{
      "type" => "terminal",
      "status" => "passed",
      "expectedHead" => @head,
      "observedHead" => @head,
      "polls" => 2
    })

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

  test "a phase-resume session enters deferred waiting and wakes once with its bounded authority" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("phase-resume-watcher")

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

    put_phase_resume_state(pid, issue, phase_budget)

    receipt_path = Path.join(workspace, "output/phase-resume-checks.jsonl")
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

      Map.has_key?(state.deferred, issue.id) and
        get_in(state.holds, [issue.id, :reason]) == "input_token_resume_pending"
    end)

    write_receipt(receipt_path, terminal_receipt("passed"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(pid)

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

    update_running_entry(pid, issue.id, %{input_token_warning_status: "delivered"})

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
    refute Process.alive?(old_waiter_pid)
  end

  test "stopping a progress-only identifier returns a clean error without crashing" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("stop-progress-only")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    Process.exit(worker_pid, :kill)

    :sys.replace_state(pid, fn state ->
      %{state | running: %{}, deferred: %{}, retry_attempts: %{}, holds: %{}}
    end)

    assert {:error, :issue_not_found} = Orchestrator.stop_issue(issue.identifier, pid)
    assert Process.alive?(pid)
  end

  test "a waiting watcher survives restart and terminal receipt persistence precedes its one wake" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-restart")

    assert {:ok, %{review_fingerprint: _review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      length(Orchestrator.snapshot(pid, 1_000).deferred) == 1
    end)

    GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert [%{identifier: identifier, watcher_state: "waiting"}] =
             Orchestrator.snapshot(restarted_pid, 1_000).deferred

    assert identifier == issue.identifier

    write_receipt(receipt_path, %{
      "type" => "terminal",
      "status" => "failed",
      "expectedHead" => @head,
      "observedHead" => @head,
      "polls" => 3
    })

    send(restarted_pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "failed" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
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

    receipt_path = Path.join(workspace, "output/checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      length(Orchestrator.snapshot(pid, 1_000).deferred) == 1
    end)

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
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)
  end

  test "a waiter exit without a terminal receipt fails closed and wakes only one diagnostic continuation" do
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

    receipt_path = Path.join(workspace, "output/checks.jsonl")

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

  test "a deferred watcher timeout wakes once with fail-closed terminal state" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("watcher-timeout")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 60)

    assert_eventually(fn ->
      get_in(:sys.get_state(pid).progress, [issue.id, "tokens_since_progress"]) == 60
    end)

    receipt_path = Path.join(workspace, "output/checks.jsonl")

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      length(Orchestrator.snapshot(pid, 1_000).deferred) == 1
    end)

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
        get_in(state.progress, [issue.id, "tokens_since_progress"]) == 60 and
        get_in(state.progress, [issue.id, "fingerprint", "hosted_receipt", "state"]) ==
          "timed_out"
    end)
  end

  test "unchanged provider polls do not reset counters but required-check changes do" do
    {pid, issue, _worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("provider-semantics",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 1_000,
        codex_no_progress_cycles: 10
      )

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    receipt_path = Path.join(workspace, "output/provider-checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    write_receipt(receipt_path, provider_receipt(1, "PENDING"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      get_in(:sys.get_state(pid).progress, [issue.id, "last_required_check_signature"]) != nil
    end)

    send_token_update(pid, issue.id, 50)

    assert_eventually(fn ->
      get_in(:sys.get_state(pid).progress, [issue.id, "tokens_since_progress"]) == 50
    end)

    write_receipt(receipt_path, provider_receipt(2, "PENDING"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})
    Process.sleep(25)

    assert get_in(:sys.get_state(pid).progress, [issue.id, "tokens_since_progress"]) == 50

    write_receipt(receipt_path, provider_receipt(3, "SUCCESS"))
    send(pid, {:poll_deferred_watcher, issue.id, watcher_token})

    assert_eventually(fn ->
      get_in(:sys.get_state(pid).progress, [issue.id, "tokens_since_progress"]) == 0
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

    receipt_path = Path.join(workspace, "output/checks.jsonl")
    File.mkdir_p!(Path.dirname(receipt_path))

    assert {:ok, %{watcher_token: watcher_token}} =
             Orchestrator.register_deferred_wait(
               issue.identifier,
               deferred_wait_attributes(workspace, receipt_path),
               pid
             )

    send(worker_pid, :finish)

    assert_eventually(fn ->
      length(Orchestrator.snapshot(pid, 1_000).deferred) == 1
    end)

    progress_path = Path.join(workspace_root, ".symphony-progress.json")
    File.rm!(progress_path)
    File.mkdir_p!(progress_path)

    write_receipt(receipt_path, %{
      "type" => "terminal",
      "status" => "passed",
      "expectedHead" => @head,
      "observedHead" => @head,
      "polls" => 2
    })

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
      case Orchestrator.snapshot(pid, 1_000).held do
        [%{identifier: identifier, reason: "progress_state_unavailable"}] ->
          identifier == issue.identifier

        _ ->
          false
      end
    end)

    refute Process.alive?(worker_pid)
    assert Orchestrator.snapshot(pid, 1_000).running == []

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

  test "startup fails closed when durable progress state cannot be trusted" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-progress-corrupt-#{System.unique_integer([:positive])}"
      )

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root
    )

    File.mkdir_p!(workspace_root)
    progress_path = Path.join(workspace_root, ".symphony-progress.json")

    File.write!(
      progress_path,
      Jason.encode!(%{
        "version" => 1,
        "issues" => %{
          "issue-corrupt" => %{
            "issue_id" => "issue-corrupt",
            "identifier" => "MT-CORRUPT",
            "fingerprint" => %{},
            "watcher" => %{"state" => "waiting"}
          }
        }
      })
    )

    File.chmod!(progress_path, 0o600)

    expected_error =
      {:error, {:progress_state_load_failed, {:progress_state_invalid, progress_path, {:invalid_progress_entry, "issue-corrupt"}}}}

    assert expected_error ==
             GenServer.start(Orchestrator, [], name: unique_orchestrator_name())

    File.rm_rf(workspace_root)
  end

  test "a no-progress hold preserves bounded-resume authority across later progress receipts" do
    {pid, issue, _worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("bounded-resume",
        codex_input_token_limit: 1_000,
        codex_no_progress_input_tokens: 100,
        codex_no_progress_cycles: 10
      )

    fingerprint = progress_fingerprint()

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    send_token_update(pid, issue.id, 100)

    assert_eventually(fn ->
      match?(
        [%{reason: "no_progress"}],
        Orchestrator.snapshot(pid, 1_000).held
      )
    end)

    assert {:ok,
            %{
              phase: "implementation",
              requested_additional_input_tokens: 500,
              effective_additional_input_tokens: 500
            }} =
             Orchestrator.resume_issue(
               issue.identifier,
               %{phase: "implementation", max_additional_input_tokens: 500},
               pid
             )

    assert [%{reason: "input_token_resume_pending", resume_phase: "implementation"}] =
             Orchestrator.snapshot(pid, 1_000).held

    provider_fingerprint =
      Map.put(fingerprint, :hosted_receipt, %{"state" => "provider_transition"})

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(provider_fingerprint, "provider-transition", progress_kind: "provider_transition"),
               pid
             )

    assert [%{reason: "input_token_resume_pending", resume_phase: "implementation"}] =
             Orchestrator.snapshot(pid, 1_000).held
  end

  defp start_progress_orchestrator(suffix, overrides \\ []) do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-progress-#{suffix}-#{System.unique_integer([:positive])}"
      )

    write_workflow_file!(
      Workflow.workflow_file_path(),
      Keyword.merge(
        [
          tracker_kind: "memory",
          workspace_root: workspace_root,
          codex_input_token_limit: nil,
          codex_no_progress_input_tokens: 1_000_000,
          codex_no_progress_cycles: 6
        ],
        overrides
      )
    )

    issue = %Issue{
      id: "issue-#{suffix}",
      identifier: "MT-#{String.upcase(suffix)}",
      title: "Progress efficiency #{suffix}",
      state: "In Progress",
      labels: []
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    {:ok, pid} = Orchestrator.start_link(name: unique_orchestrator_name())
    wait_for_initial_poll(pid)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    worker_pid = spawn(fn -> wait_for_finish() end)
    workspace = Path.join(workspace_root, issue.identifier)
    File.mkdir_p!(workspace)
    install_waiter_fixture!(workspace)
    put_running_entry(pid, issue, worker_pid, workspace, overrides)

    on_exit(fn ->
      if Process.alive?(worker_pid), do: Process.exit(worker_pid, :shutdown)

      if Process.alive?(pid) do
        try do
          GenServer.stop(pid)
        catch
          :exit, _reason -> :ok
        end
      end

      File.rm_rf(workspace_root)
    end)

    {pid, issue, worker_pid, workspace_root, workspace}
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

  defp unique_orchestrator_name do
    Module.concat(
      __MODULE__,
      String.to_atom("Orchestrator#{System.unique_integer([:positive])}")
    )
  end

  defp put_running_entry(pid, issue, worker_pid, workspace, overrides) do
    input_token_limit = Keyword.get(overrides, :codex_input_token_limit)

    entry = %{
      pid: worker_pid,
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      worker_affinity: :local,
      workspace_path: workspace,
      session_id: "session-0",
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
      input_token_limit: input_token_limit,
      input_token_tier_limit: input_token_limit,
      input_token_warning_ratio: 0.70,
      input_token_checkpoint_grace: 500_000,
      input_token_warning_sent: false,
      input_token_warning_status: nil,
      input_token_warning_threshold: nil,
      input_token_warning_observed_at: nil,
      input_token_warning_ack_timer_ref: nil,
      input_token_warning_ack_token: nil,
      input_token_warning_reader_busy: false,
      resume_phase: nil,
      requested_additional_input_tokens: nil,
      effective_additional_input_tokens: nil,
      attempt_input_token_baseline: 0,
      turn_count: 0,
      retry_attempt: 0,
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn state ->
      monitored_entry = Map.put(entry, :ref, Process.monitor(worker_pid))

      %{
        state
        | running: %{issue.id => monitored_entry},
          retry_attempts: %{},
          claimed: MapSet.put(state.claimed, issue.id)
      }
    end)
  end

  defp progress_fingerprint(overrides \\ []) do
    head = Keyword.get(overrides, :head, @head)

    %{
      contract_revision: Keyword.get(overrides, :contract_revision, "SYMPHONY-PROGRESS-EFFICIENCY-v2"),
      base_sha: @head,
      head_sha: head,
      diff_checksum: Keyword.get(overrides, :diff_checksum, "diff-1"),
      matrix_checksum: Keyword.get(overrides, :matrix_checksum, "matrix-v2"),
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

  defp install_waiter_fixture!(workspace) do
    script = Path.join(workspace, "waiter-fixture.sh")
    File.write!(script, "#!/bin/sh\nwhile :; do sleep 60; done\n")
    File.chmod!(script, 0o700)
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

  defp send_token_update(pid, issue_id, input_tokens) do
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

  defp begin_new_attempt(pid, issue_id) do
    update_running_entry(pid, issue_id, %{
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      turn_count: 0,
      started_at: DateTime.utc_now()
    })
  end

  defp put_phase_resume_state(pid, issue, phase_budget) do
    :sys.replace_state(pid, fn state ->
      running_entry =
        state.running
        |> Map.fetch!(issue.id)
        |> Map.merge(%{
          resume_phase: phase_budget.phase,
          requested_additional_input_tokens: phase_budget.requested_additional_input_tokens,
          effective_additional_input_tokens: phase_budget.effective_additional_input_tokens
        })

      hold = %{
        issue_id: issue.id,
        identifier: issue.identifier,
        reason: "input_token_resume_pending",
        resume_phase: phase_budget.phase,
        requested_additional_input_tokens: phase_budget.requested_additional_input_tokens,
        effective_additional_input_tokens: phase_budget.effective_additional_input_tokens,
        cleanup_pending: false
      }

      %{
        state
        | running: Map.put(state.running, issue.id, running_entry),
          holds: Map.put(state.holds, issue.id, hold)
      }
    end)
  end

  defp update_running_entry(pid, issue_id, updates) do
    :sys.replace_state(pid, fn state ->
      running_entry = state.running |> Map.fetch!(issue_id) |> Map.merge(updates)
      %{state | running: Map.put(state.running, issue_id, running_entry)}
    end)
  end

  defp waiter_task_pid(state, issue_id) do
    case Map.fetch!(state.waiter_tasks, issue_id) do
      pid when is_pid(pid) -> pid
      %{pid: pid} when is_pid(pid) -> pid
    end
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

  defp provider_receipt(poll, build_state) do
    %{
      "type" => "provider",
      "poll" => poll,
      "observedAt" => "2026-07-24T00:00:0#{poll}Z",
      "provider" => %{
        "headRefOid" => @head,
        "statusCheckRollup" => [
          %{
            "name" => "Build Check",
            "conclusion" => if(build_state == "SUCCESS", do: "SUCCESS", else: nil),
            "state" => build_state,
            "status" => if(build_state == "SUCCESS", do: "COMPLETED", else: "IN_PROGRESS")
          },
          %{
            "name" => "Test & Lint",
            "conclusion" => nil,
            "state" => "PENDING",
            "status" => "IN_PROGRESS"
          },
          %{
            "name" => "non-required-#{poll}",
            "conclusion" => nil,
            "state" => "PENDING",
            "status" => "IN_PROGRESS"
          }
        ]
      }
    }
  end

  defp send_session_started(pid, issue_id, session_id) do
    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :session_started,
         session_id: session_id,
         thread_id: "thread",
         turn_id: session_id,
         payload: %{session_id: session_id},
         timestamp: DateTime.utc_now()
       }}
    )
  end

  defp write_receipt(path, receipt) do
    File.write!(path, Jason.encode!(receipt) <> "\n")
  end

  defp wait_for_finish do
    receive do
      :finish -> :ok
    end
  end

  defp wait_for_initial_poll(pid, attempts \\ 80)

  defp wait_for_initial_poll(pid, attempts) when attempts > 0 do
    case Orchestrator.snapshot(pid, 1_000) do
      %{polling: %{checking?: false, next_poll_in_ms: next_poll}}
      when is_integer(next_poll) and next_poll > 0 ->
        :ok

      _ ->
        Process.sleep(25)
        wait_for_initial_poll(pid, attempts - 1)
    end
  end

  defp wait_for_initial_poll(_pid, 0), do: flunk("initial orchestrator poll did not finish")

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
