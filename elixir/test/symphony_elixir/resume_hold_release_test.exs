defmodule SymphonyElixir.ResumeHoldReleaseTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.HoldStore

  defp pending_hold(issue, workspace) do
    %{
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
  end

  defp base_state(workspace_root) do
    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      tracker_active_states: ["Todo", "In Progress", "In Review"],
      tracker_terminal_states: ["Closed", "Cancelled", "Canceled", "Duplicate"]
    )

    File.mkdir_p!(workspace_root)
    :ok
  end

  test "reaping a running issue on a non-active tracker state clears the armed-resume hold" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-resume-clear-run-#{System.unique_integer([:positive])}"
      )

    try do
      :ok = base_state(test_root)

      issue = %Issue{id: "issue-1", identifier: "MT-901", state: "In Progress", labels: []}
      workspace = Path.join(test_root, issue.identifier)
      File.mkdir_p!(workspace)

      hold = pending_hold(issue, workspace)
      assert :ok = HoldStore.persist(test_root, %{issue.id => hold})

      agent_pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      state = %Orchestrator.State{
        running: %{
          issue.id => %{
            pid: agent_pid,
            ref: nil,
            identifier: issue.identifier,
            issue: issue,
            started_at: DateTime.utc_now()
          }
        },
        holds: %{issue.id => hold},
        claimed: MapSet.new([issue.id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      blocked_issue = %{issue | state: "Blocked"}
      updated_state = Orchestrator.reconcile_issue_states_for_test([blocked_issue], state)

      refute Map.has_key?(updated_state.running, issue.id)
      refute MapSet.member?(updated_state.claimed, issue.id)
      refute Map.has_key?(updated_state.holds, issue.id)

      assert {:ok, persisted} = HoldStore.load(test_root)
      refute Map.has_key?(persisted, issue.id)
    after
      File.rm_rf(test_root)
    end
  end

  test "releasing a blocked claim clears the armed-resume hold and restores claimability" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-resume-clear-blocked-#{System.unique_integer([:positive])}"
      )

    try do
      :ok = base_state(test_root)

      issue = %Issue{id: "issue-2", identifier: "MT-902", state: "Blocked", labels: []}
      workspace = Path.join(test_root, issue.identifier)
      File.mkdir_p!(workspace)

      hold = pending_hold(issue, workspace)
      assert :ok = HoldStore.persist(test_root, %{issue.id => hold})

      state = %Orchestrator.State{
        max_concurrent_agents: 1,
        blocked: %{
          issue.id => %{
            issue_id: issue.id,
            identifier: issue.identifier,
            issue: issue,
            workspace_path: workspace,
            error: "operator-commit-lane",
            blocked_at: DateTime.utc_now()
          }
        },
        holds: %{issue.id => hold},
        claimed: MapSet.new([issue.id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      backlog_issue = %{issue | state: "Backlog"}
      updated_state = Orchestrator.reconcile_blocked_issue_states_for_test([backlog_issue], state)

      refute Map.has_key?(updated_state.blocked, issue.id)
      refute MapSet.member?(updated_state.claimed, issue.id)
      refute Map.has_key?(updated_state.holds, issue.id)

      assert {:ok, persisted} = HoldStore.load(test_root)
      refute Map.has_key?(persisted, issue.id)
    after
      File.rm_rf(test_root)
    end
  end

  test "durable budget holds survive claim release unchanged" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-budget-hold-keep-#{System.unique_integer([:positive])}"
      )

    try do
      :ok = base_state(test_root)

      issue = %Issue{id: "issue-3", identifier: "MT-903", state: "Blocked", labels: []}
      workspace = Path.join(test_root, issue.identifier)
      File.mkdir_p!(workspace)

      hold =
        pending_hold(issue, workspace)
        |> Map.put(:reason, "input_token_checkpoint")
        |> Map.put(:observed_tokens, 90)

      assert :ok = HoldStore.persist(test_root, %{issue.id => hold})

      state = %Orchestrator.State{
        blocked: %{
          issue.id => %{
            issue_id: issue.id,
            identifier: issue.identifier,
            issue: issue,
            workspace_path: workspace,
            error: "checkpoint",
            blocked_at: DateTime.utc_now()
          }
        },
        holds: %{issue.id => hold},
        claimed: MapSet.new([issue.id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      backlog_issue = %{issue | state: "Backlog"}
      updated_state = Orchestrator.reconcile_blocked_issue_states_for_test([backlog_issue], state)

      refute Map.has_key?(updated_state.blocked, issue.id)
      assert updated_state.holds[issue.id].reason == "input_token_checkpoint"

      assert {:ok, persisted} = HoldStore.load(test_root)
      assert persisted[issue.id].reason == "input_token_checkpoint"
    after
      File.rm_rf(test_root)
    end
  end

  test "scope reconciliation durably clears holds that are no longer visible" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-held-scope-clear-#{System.unique_integer([:positive])}"
      )

    try do
      :ok = base_state(test_root)

      issue = %Issue{id: "issue-4", identifier: "MT-904", state: "Todo", labels: []}
      workspace = Path.join(test_root, issue.identifier)
      File.mkdir_p!(workspace)

      hold =
        pending_hold(issue, workspace)
        |> Map.put(:reason, "input_token_checkpoint")

      assert :ok = HoldStore.persist(test_root, %{issue.id => hold})

      state = %Orchestrator.State{
        holds: %{issue.id => hold},
        claimed: MapSet.new([issue.id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      updated_state = Orchestrator.reconcile_held_issue_scope_for_test([], state)

      refute Map.has_key?(updated_state.holds, issue.id)
      refute MapSet.member?(updated_state.claimed, issue.id)
      assert updated_state.hold_store_available
      assert File.exists?(workspace)

      assert {:ok, persisted} = HoldStore.load(test_root)
      refute Map.has_key?(persisted, issue.id)
    after
      File.rm_rf(test_root)
    end
  end

  test "scope reconciliation preserves holds that remain visible" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-held-scope-keep-#{System.unique_integer([:positive])}"
      )

    try do
      :ok = base_state(test_root)

      issue = %Issue{id: "issue-5", identifier: "MT-905", state: "Todo", labels: []}
      workspace = Path.join(test_root, issue.identifier)
      File.mkdir_p!(workspace)

      hold =
        pending_hold(issue, workspace)
        |> Map.put(:reason, "manual_stop")

      assert :ok = HoldStore.persist(test_root, %{issue.id => hold})

      state = %Orchestrator.State{
        holds: %{issue.id => hold},
        claimed: MapSet.new([issue.id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      updated_state = Orchestrator.reconcile_held_issue_scope_for_test([issue], state)

      assert updated_state.holds[issue.id] == hold
      assert MapSet.member?(updated_state.claimed, issue.id)

      assert {:ok, persisted} = HoldStore.load(test_root)
      assert persisted[issue.id].reason == "manual_stop"
    after
      File.rm_rf(test_root)
    end
  end
end
