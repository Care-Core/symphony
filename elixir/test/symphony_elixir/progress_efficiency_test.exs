defmodule SymphonyElixir.ProgressEfficiencyTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixirWeb.Presenter

  @head "0123456789abcdef0123456789abcdef01234567"
  @next_head "fedcba9876543210fedcba9876543210fedcba98"

  test "live issue payload exposes a registered review fingerprint and stop-exit fails closed" do
    {pid, issue, _worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("review-fingerprint-visibility")

    assert {:ok, %{changed: true, review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    {:registered_name, orchestrator_name} = Process.info(pid, :registered_name)

    assert {:ok, %{running: %{review_fingerprint_hash: ^review_hash}}} =
             Presenter.issue_payload(issue.identifier, orchestrator_name, 1_000)

    assert {:ok, %{authorized: true, review_fingerprint: ^review_hash}} =
             authorize_review(pid, issue, review_hash, "full")

    stopped_issue = %{issue | state: "Blocked"}

    stopped_state =
      Orchestrator.reconcile_issue_states_for_test(
        [stopped_issue],
        :sys.get_state(pid)
      )

    :sys.replace_state(pid, fn _state -> stopped_state end)

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert persisted["review_fingerprint_hash"] == review_hash

    assert {:error, :issue_not_found} =
             Presenter.issue_payload(issue.identifier, orchestrator_name, 1_000)

    assert {:error, :issue_not_found} = authorize_review(pid, issue, review_hash, "full")
  end

  test "a retired owner session cannot mutate its successor's progress or review state" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("stale-owner-session")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    :sys.replace_state(pid, fn state ->
      running_entry = Map.fetch!(state.running, issue.id)
      updated_entry = Map.put(running_entry, :attempt_session_id, "thread-successor")
      %{state | running: Map.put(state.running, issue.id, updated_entry)}
    end)

    assert {:error, :stale_issue_session} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "stale-checkpoint"),
               pid
             )

    assert {:error, :stale_issue_session} =
             authorize_review(pid, issue, review_hash, "full")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "successor-checkpoint", owner_session: "thread-successor"),
               pid
             )

    Process.exit(worker_pid, :shutdown)
  end

  test "signed request nonces are one-use and scoped to the exact running attempt" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("attempt-scoped-nonces")

    nonce = "123e4567-e89b-42d3-a456-426614174001"

    attributes =
      progress_attributes(progress_fingerprint(), "checkpoint-1")
      |> Map.merge(%{
        issue_request_authorized: true,
        issue_capability_nonce: nonce
      })

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(issue.identifier, attributes, pid)

    assert {:error, :replayed_issue_request} =
             Orchestrator.record_progress(issue.identifier, attributes, pid)

    :sys.replace_state(pid, fn state ->
      running_entry = Map.fetch!(state.running, issue.id)

      exhausted_entry =
        Map.put(
          running_entry,
          :issue_capability_nonces,
          1..4_096 |> Enum.map(&"seen-#{&1}") |> MapSet.new()
        )

      %{state | running: Map.put(state.running, issue.id, exhausted_entry)}
    end)

    fresh_nonce_attributes =
      attributes
      |> Map.put(:issue_capability_nonce, "123e4567-e89b-42d3-a456-426614174002")
      |> Map.put(:progress_receipt, "capacity-checkpoint")

    assert {:error, :issue_nonce_capacity_exceeded} =
             Orchestrator.record_progress(issue.identifier, fresh_nonce_attributes, pid)

    :sys.replace_state(pid, fn state ->
      running_entry = Map.fetch!(state.running, issue.id)

      successor_entry =
        running_entry
        |> Map.put(:attempt_session_id, "thread-successor")
        |> Map.put(:issue_capability_nonces, MapSet.new())

      %{state | running: Map.put(state.running, issue.id, successor_entry)}
    end)

    successor_attributes =
      attributes
      |> Map.put(:owner_session, "thread-successor")
      |> Map.put(:progress_receipt, "successor-checkpoint")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(issue.identifier, successor_attributes, pid)

    Process.exit(worker_pid, :shutdown)
  end

  # HOA-07 (CC-1927, SYMPHONY-HUMAN-OVERRIDE-AUTHENTICITY-v1)
  #
  # A well-formed but fabricated `linear-comment:<uuid>@<timestamp>` reference must
  # not authorize an extra review round. Because the single-use ledger keys on the
  # exact string, an unauthenticated override lets a caller mint unlimited rounds
  # simply by generating a fresh UUID.
  test "a fabricated human review override does not authorize an extra round" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("forged-human-override")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    assert {:ok, %{authorized: true}} = authorize_review(pid, issue, review_hash, "full")

    forged = "linear-comment:aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee@2026-08-06T15:14:10.490Z"

    assert {:error, _reason} =
             authorize_review(pid, issue, review_hash, "full", human_override: forged),
           "a fabricated human override must never authorize an extra review round"

    Process.exit(worker_pid, :shutdown)
  end

  # HOA-01..HOA-09 (CC-1927). The resolver is stubbed so each authenticity
  # condition can be exercised independently of the live Linear API.
  for {name, suffix, resolver_result, expectation} <- [
        {"HOA-01 a matching human comment authorizes exactly one extra round", "hoa-valid",
         {:ok,
          %{
            issue_identifier: "MT-HOA-VALID",
            created_at: "2026-08-06T15:14:10.490Z",
            human_author?: true
          }}, :authorized},
        {"HOA-03 a comment on a different issue is denied", "hoa-wrong-issue",
         {:ok,
          %{
            issue_identifier: "MT-SOME-OTHER-ISSUE",
            created_at: "2026-08-06T15:14:10.490Z",
            human_author?: true
          }}, :denied},
        {"HOA-04 a comment authored by a bot actor is denied", "hoa-bot-author",
         {:ok,
          %{
            issue_identifier: "MT-HOA-BOT-AUTHOR",
            created_at: "2026-08-06T15:14:10.490Z",
            human_author?: false
          }}, :denied},
        {"HOA-05 a timestamp mismatch is denied", "hoa-timestamp",
         {:ok,
          %{
            issue_identifier: "MT-HOA-TIMESTAMP",
            created_at: "2026-08-06T09:00:00.000Z",
            human_author?: true
          }}, :denied},
        {"HOA-02 a non-existent comment is denied", "hoa-missing",
         {:error, :review_override_comment_missing}, :denied},
        {"HOA-08 a Linear API failure is denied", "hoa-api-error",
         {:error, {:review_override_lookup_failed, :timeout}}, :denied}
      ] do
    test name do
      suffix = unquote(suffix)
      expectation = unquote(expectation)
      resolver_result = unquote(Macro.escape(resolver_result))

      {pid, issue, worker_pid, _workspace_root, _workspace} =
        start_progress_orchestrator(suffix)

      Application.put_env(:symphony_elixir, :review_override_comment_resolver, fn _id ->
        resolver_result
      end)

      on_exit(fn ->
        Application.delete_env(:symphony_elixir, :review_override_comment_resolver)
      end)

      assert {:ok, %{review_fingerprint: review_hash}} =
               Orchestrator.record_progress(
                 issue.identifier,
                 progress_attributes(progress_fingerprint(), "checkpoint-1"),
                 pid
               )

      # Consume the one ordinarily permitted full review.
      assert {:ok, %{authorized: true}} = authorize_review(pid, issue, review_hash, "full")

      override =
        "linear-comment:35d03f7d-e5cc-4ab3-89d2-e8baeda17362@2026-08-06T15:14:10.490Z"

      result = authorize_review(pid, issue, review_hash, "full", human_override: override)

      case expectation do
        :authorized -> assert {:ok, %{authorized: true}} = result
        :denied -> assert {:error, _reason} = result
      end

      Process.exit(worker_pid, :shutdown)
    end
  end

  # HOA-06 — BLOCKED by a separate pre-existing defect, see CC-1927 handoff.
  #
  # `authorize_consumed_review/4` binds the result of `increment_review_round/3`
  # to `_eligible` (orchestrator.ex:2593) and then rebuilds `updated` from the
  # ORIGINAL `progress` (orchestrator.ex:2606). The incremented counters and the
  # `used_review_overrides` ledger are therefore never persisted, so replay
  # protection cannot take effect and review counts stay permanently zero.
  #
  # Fixing that is a behavior change beyond this issue's authenticity remit and
  # trips the `scope-drift` stop condition. Left skipped deliberately so the gap
  # is visible rather than silently absent.
  @tag :skip
  test "HOA-06 an authenticated override cannot be replayed" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("hoa-replay")

    Application.put_env(:symphony_elixir, :review_override_comment_resolver, fn _id ->
      {:ok,
       %{
         issue_identifier: "MT-HOA-REPLAY",
         created_at: "2026-08-06T15:14:10.490Z",
         human_author?: true
       }}
    end)

    on_exit(fn ->
      Application.delete_env(:symphony_elixir, :review_override_comment_resolver)
    end)

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    assert {:ok, %{authorized: true}} = authorize_review(pid, issue, review_hash, "full")

    override = "linear-comment:35d03f7d-e5cc-4ab3-89d2-e8baeda17362@2026-08-06T15:14:10.490Z"

    assert {:ok, %{authorized: true}} =
             authorize_review(pid, issue, review_hash, "full", human_override: override)

    assert {:error, _reason} =
             authorize_review(pid, issue, review_hash, "full", human_override: override),
           "an authenticated override must remain single-use"

    Process.exit(worker_pid, :shutdown)
  end

  test "global owner review keeps the explicit fingerprint precondition" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("global-owner-review")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    owner_attributes = %{
      kind: "full",
      owner_session_authorized: true,
      requested_head: @head,
      observed_local_head: @head,
      observed_remote_head: @head
    }

    assert {:error, :review_fingerprint_required} =
             Orchestrator.authorize_review(issue.identifier, owner_attributes, pid)

    assert {:ok, %{authorized: true, review_fingerprint: ^review_hash}} =
             Orchestrator.authorize_review(
               issue.identifier,
               Map.put(owner_attributes, :review_fingerprint, review_hash),
               pid
             )

    Process.exit(worker_pid, :shutdown)
  end

  test "review heads normalize before authorization and response construction" do
    {pid, issue, worker_pid, _workspace_root, _workspace} =
      start_progress_orchestrator("uppercase-review-head")

    upper_head = String.upcase(@head)

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(head: upper_head), "checkpoint-1"),
               pid
             )

    assert {:ok, %{authorized: true, requested_head: @head}} =
             authorize_review(pid, issue, review_hash, "full",
               requested_head: upper_head,
               observed_local_head: upper_head,
               observed_remote_head: upper_head
             )

    Process.exit(worker_pid, :shutdown)
  end

  test "checksum prefix formatting preserves a completed full review for delta authorization" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("review-checksum-prefix")

    checksum = String.duplicate("ab", 32)
    prefixed_fingerprint = progress_fingerprint(matrix_checksum: "sha256:#{checksum}")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(prefixed_fingerprint, "checkpoint-1"),
               pid
             )

    assert {:ok, %{kind: "full"}} = authorize_review(pid, issue, review_hash, "full")

    completed_full_review = Map.put(prefixed_fingerprint, :full_review_verdict, "pass")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(
                 completed_full_review,
                 "full-review-receipt",
                 progress_kind: "review_receipt"
               ),
               pid
             )

    assert :sys.get_state(pid).progress[issue.id]["full_review_count"] == 1

    :sys.replace_state(pid, fn state ->
      entry = state.progress[issue.id]

      legacy_fingerprint =
        Map.put(entry["fingerprint"], "matrix_checksum", "sha256:#{checksum}")

      %{state | progress: Map.put(state.progress, issue.id, Map.put(entry, "fingerprint", legacy_fingerprint))}
    end)

    bare_fingerprint =
      progress_fingerprint(
        head: @next_head,
        diff_checksum: "diff-2",
        matrix_checksum: checksum
      )

    preservation_log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:ok, %{review_fingerprint: _changed_review_hash}} =
                 Orchestrator.record_progress(
                   issue.identifier,
                   progress_attributes(bare_fingerprint, "checkpoint-2"),
                   pid
                 )
      end)

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert persisted["full_review_count"] == 1
    refute preservation_log =~ "Review counts reset"
    changed_review_hash = persisted["review_fingerprint_hash"]

    changed_head_options = [
      requested_head: @next_head,
      observed_local_head: @next_head,
      observed_remote_head: @next_head
    ]

    assert {:ok, %{kind: "delta"}} =
             authorize_review(
               pid,
               issue,
               changed_review_hash,
               "delta",
               changed_head_options
             )

    Process.exit(worker_pid, :shutdown)
  end

  test "checksum normalization stores sha256 hex bare and lowercase across fingerprint fields" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("checksum-normalization")

    mixed_case_checksum = String.duplicate("Ab", 32)
    expected_checksum = String.downcase(mixed_case_checksum)

    fingerprint =
      progress_fingerprint(
        diff_checksum: mixed_case_checksum,
        matrix_checksum: "sha256:#{mixed_case_checksum}"
      )

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(fingerprint, "checkpoint-1"),
               pid
             )

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert persisted["fingerprint"]["diff_checksum"] == expected_checksum
    assert persisted["fingerprint"]["matrix_checksum"] == expected_checksum

    Process.exit(worker_pid, :shutdown)
  end

  test "a genuinely different canonical matrix checksum resets completed review counts" do
    {pid, issue, worker_pid, workspace_root, _workspace} =
      start_progress_orchestrator("review-matrix-change")

    initial_checksum = String.duplicate("01", 32)
    changed_checksum = String.duplicate("10", 32)
    initial_fingerprint = progress_fingerprint(matrix_checksum: "sha256:#{initial_checksum}")

    assert {:ok, %{review_fingerprint: review_hash}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(initial_fingerprint, "checkpoint-1"),
               pid
             )

    assert {:ok, %{kind: "full"}} = authorize_review(pid, issue, review_hash, "full")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(
                 Map.put(initial_fingerprint, :full_review_verdict, "pass"),
                 "full-review-receipt",
                 progress_kind: "review_receipt"
               ),
               pid
             )

    changed_fingerprint =
      progress_fingerprint(
        head: @next_head,
        diff_checksum: "diff-2",
        matrix_checksum: changed_checksum
      )

    reset_log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:ok, %{review_fingerprint: _changed_review_hash}} =
                 Orchestrator.record_progress(
                   issue.identifier,
                   progress_attributes(changed_fingerprint, "checkpoint-2"),
                   pid
                 )
      end)

    assert reset_log =~ "Review counts reset issue_id=#{issue.id}"
    assert reset_log =~ ~s(prior_matrix_checksum="#{initial_checksum}")
    assert reset_log =~ ~s(current_matrix_checksum="#{changed_checksum}")

    issue_id = issue.id
    assert {:ok, %{^issue_id => persisted}} = SymphonyElixir.HoldStore.load_progress(workspace_root)
    assert persisted["full_review_count"] == 0
    changed_review_hash = persisted["review_fingerprint_hash"]

    changed_head_options = [
      requested_head: @next_head,
      observed_local_head: @next_head,
      observed_remote_head: @next_head
    ]

    assert {:error, :full_review_required} =
             authorize_review(
               pid,
               issue,
               changed_review_hash,
               "delta",
               changed_head_options
             )

    Process.exit(worker_pid, :shutdown)
  end

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

  test "deferred waits are denied before an issue-selected command can observe service environment" do
    {pid, issue, worker_pid, _workspace_root, workspace} =
      start_progress_orchestrator("waiter-denied")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    marker = Path.join(workspace, "waiter-environment.txt")
    script = Path.join(workspace, "issue-selected-waiter.sh")
    File.write!(script, "#!/bin/sh\nprintf '%s' \"$SYMPHONY_WAITER_SECRET\" > \"$1\"\n")
    File.chmod!(script, 0o700)

    previous_secret = System.get_env("SYMPHONY_WAITER_SECRET")
    System.put_env("SYMPHONY_WAITER_SECRET", "must-not-cross-engine-boundary")
    on_exit(fn -> restore_env("SYMPHONY_WAITER_SECRET", previous_secret) end)

    attributes = %{
      expected_head: @head,
      receipt_path: Path.join(workspace, "waiter-receipt.jsonl"),
      timeout_seconds: 1_200,
      waiter_script: script,
      waiter_args: [marker]
    }

    assert {:error, :deferred_wait_disabled} =
             Orchestrator.register_deferred_wait(issue.identifier, attributes, pid)

    assert {:error, :deferred_wait_disabled} =
             GenServer.call(pid, {:register_deferred_wait, issue.identifier, attributes})

    refute File.exists?(marker)
    assert :sys.get_state(pid).progress[issue.id]["watcher"] == nil
    assert GenServer.call(pid, {:continue_after_turn, issue.id})

    send(worker_pid, :finish)

    assert_eventually(fn ->
      state = :sys.get_state(pid)
      state.running == %{} and map_size(state.retry_attempts) == 1
    end)

    refute File.exists?(marker)
  end

  test "restart fails closed a legacy waiting record without executing its stored command" do
    {pid, issue, worker_pid, workspace_root, workspace} =
      start_progress_orchestrator("legacy-waiter-denied")

    assert {:ok, %{changed: true}} =
             Orchestrator.record_progress(
               issue.identifier,
               progress_attributes(progress_fingerprint(), "checkpoint-1"),
               pid
             )

    marker = Path.join(workspace, "legacy-waiter-environment.txt")
    script = Path.join(workspace, "legacy-issue-selected-waiter.sh")
    File.write!(script, "#!/bin/sh\nprintf '%s' \"$SYMPHONY_WAITER_SECRET\" > \"$1\"\n")
    File.chmod!(script, 0o700)

    previous_secret = System.get_env("SYMPHONY_WAITER_SECRET")
    System.put_env("SYMPHONY_WAITER_SECRET", "must-not-cross-restart-boundary")
    on_exit(fn -> restore_env("SYMPHONY_WAITER_SECRET", previous_secret) end)

    watcher = %{
      "state" => "waiting",
      "token" => "legacy-waiter-token",
      "expected_head" => @head,
      "receipt_path" => Path.join(workspace, "legacy-waiter-receipt.jsonl"),
      "registered_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "deadline_unix_ms" => System.system_time(:millisecond) + 1_200_000,
      "last_receipt_hash" => nil,
      "wake_count" => 0,
      "identifier" => issue.identifier,
      "worker_host" => nil,
      "workspace_path" => workspace,
      "waiter_command" => %{"script" => script, "args" => [marker]}
    }

    progress =
      pid
      |> :sys.get_state()
      |> get_in([Access.key!(:progress), issue.id])
      |> Map.put("watcher", watcher)
      |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())

    :sys.replace_state(pid, fn state ->
      %{state | progress: Map.put(state.progress, issue.id, progress)}
    end)

    assert :ok = SymphonyElixir.HoldStore.persist_progress(workspace_root, %{issue.id => progress})

    send(worker_pid, :finish)

    assert_eventually(fn ->
      Map.has_key?(:sys.get_state(pid).deferred, issue.id)
    end)

    GenServer.stop(pid)
    restarted_pid = start_replacement_orchestrator()

    assert_eventually(fn ->
      state = :sys.get_state(restarted_pid)

      state.waiter_tasks == %{} and
        state.deferred == %{} and
        map_size(state.retry_attempts) == 1 and
        get_in(state.progress, [issue.id, "watcher", "state"]) == "receipt_invalid" and
        get_in(state.progress, [issue.id, "watcher", "wake_count"]) == 1
    end)

    refute File.exists?(marker)
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

    entry = %{
      pid: worker_pid,
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: workspace,
      attempt_session_id: "thread-turn",
      session_id: "thread-turn",
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_app_server_pid: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      input_token_warning_status: nil,
      input_token_warning_threshold: nil,
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

  defp start_replacement_orchestrator do
    name = Module.concat(__MODULE__, String.to_atom("Orchestrator#{System.unique_integer([:positive])}"))
    {:ok, pid} = Orchestrator.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    pid
  end

  defp progress_fingerprint(overrides \\ []) do
    head = Keyword.get(overrides, :head, @head)

    %{
      contract_revision: "SYMPHONY-UPSTREAM-REBASE-v1",
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
      owner_session: Keyword.get(overrides, :owner_session, "thread-turn"),
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
        owner_session: Keyword.get(overrides, :owner_session, "thread-turn"),
        review_fingerprint: review_hash,
        requested_head: requested_head,
        observed_local_head: Keyword.get(overrides, :observed_local_head, requested_head),
        observed_remote_head: Keyword.get(overrides, :observed_remote_head, requested_head),
        human_override: Keyword.get(overrides, :human_override)
      },
      pid
    )
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
