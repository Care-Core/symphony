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
