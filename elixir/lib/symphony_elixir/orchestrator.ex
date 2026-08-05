defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls the configured issue tracker and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{AgentRunner, Config, HoldStore, StatusDashboard, Tracker, Workspace}
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.Tracker.Issue

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  @phase_resume_pending_reason "input_token_resume_pending"
  @resume_phases ~w(implementation validation review-fix hosted-closeout landing)
  @progress_fingerprint_fields ~w(contract_revision base_sha head_sha diff_checksum matrix_checksum required_check_set latest_human_comment_checkpoint full_review_verdict hosted_receipt)
  @max_issue_request_nonces 4_096
  @review_fingerprint_fields ~w(contract_revision base_sha head_sha diff_checksum matrix_checksum required_check_set latest_human_comment_checkpoint)
  @review_kinds ~w(full delta security)
  @sha256_checksum_pattern ~r/\A(?:sha256:)?([0-9a-f]{64})\z/i
  @human_override_pattern ~r/\Alinear-comment:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}@[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+Z\z/i
  @terminal_watcher_states ~w(passed failed timed_out head_changed receipt_invalid cancelled)
  @watcher_poll_interval_ms 5_000
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :tick_timer_ref,
      :tick_token,
      :agent_runner,
      task_supervisor: SymphonyElixir.TaskSupervisor,
      running: %{},
      deferred: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      blocked: %{},
      retry_attempts: %{},
      holds: %{},
      hold_store_available: true,
      progress: %{},
      progress_state_available: true,
      waiter_tasks: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    case Config.settings() do
      {:ok, config} ->
        init_with_config(config, opts)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp init_with_config(config, opts) do
    workspace_root = Config.local_workspace_root()

    with {:ok, holds} <- HoldStore.load(workspace_root),
         {:ok, progress} <- HoldStore.load_progress(workspace_root) do
      state = initial_state(config, opts, holds, progress)

      if map_size(holds) > 0 do
        Logger.info("Restored durable issue holds count=#{map_size(holds)}")
      end

      run_terminal_workspace_cleanup()
      {:ok, state |> disable_restored_waiters() |> schedule_tick(0)}
    else
      {:error, {:progress_state_invalid, _, _} = reason} ->
        Logger.error("Failed to restore durable progress state: #{inspect(reason)}")
        {:stop, {:progress_state_load_failed, reason}}

      {:error, {:progress_state_invalid_file, _, _} = reason} ->
        Logger.error("Failed to restore durable progress state: #{inspect(reason)}")
        {:stop, {:progress_state_load_failed, reason}}

      {:error, {:progress_state_unreadable, _, _} = reason} ->
        Logger.error("Failed to restore durable progress state: #{inspect(reason)}")
        {:stop, {:progress_state_load_failed, reason}}

      {:error, reason} ->
        Logger.error("Failed to restore durable issue state: #{inspect(reason)}")
        {:stop, {:hold_state_load_failed, reason}}
    end
  end

  defp initial_state(config, opts, holds, progress) do
    waiting_issue_ids = waiting_watcher_issue_ids(progress)

    %State{
      poll_interval_ms: config.polling.interval_ms,
      max_concurrent_agents: config.agent.max_concurrent_agents,
      next_poll_due_at_ms: System.monotonic_time(:millisecond),
      poll_check_in_progress: false,
      tick_timer_ref: nil,
      tick_token: nil,
      agent_runner: Keyword.get(opts, :agent_runner, &AgentRunner.run/3),
      task_supervisor: Keyword.get(opts, :task_supervisor, SymphonyElixir.TaskSupervisor),
      holds: holds,
      progress: progress,
      deferred: restore_deferred_watchers(progress),
      claimed: MapSet.new(Map.keys(holds) ++ waiting_issue_ids),
      codex_totals: @empty_codex_totals,
      codex_rate_limits: nil
    }
  end

  @impl true
  def handle_info({:tick, tick_token}, %{tick_token: tick_token} = state)
      when is_reference(tick_token) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = maybe_dispatch(state)
    state = schedule_tick(state, state.poll_interval_ms)
    state = %{state | poll_check_in_progress: false}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        {:noreply, state}

      issue_id ->
        {running_entry, state} = pop_running_entry(state, issue_id)
        state = record_session_completion_totals(state, running_entry)
        session_id = running_entry_session_id(running_entry)

        state = handle_agent_down(reason, state, issue_id, running_entry, session_id)

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, %{running: running} = state)
      when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        updated_running_entry =
          running_entry
          |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
          |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])

        notify_dashboard()
        {:noreply, %{state | running: Map.put(running, issue_id, updated_running_entry)}}
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)
        updated_running_entry = settle_input_token_warning_response(updated_running_entry, update.event)

        state =
          state
          |> apply_codex_token_delta(token_delta)
          |> apply_codex_rate_limits(update)

        state = %{state | running: Map.put(running, issue_id, updated_running_entry)}
        state = enforce_input_token_budget(state, issue_id)

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:retry_issue, issue_id, retry_token}, state) do
    result =
      case pop_retry_attempt_state(state, issue_id, retry_token) do
        {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
        :missing -> {:noreply, state}
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info({:poll_deferred_watcher, issue_id, watcher_token}, state) do
    state = poll_deferred_watcher(state, issue_id, watcher_token)
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:deferred_waiter_exit, issue_id, watcher_token, result}, state) do
    state =
      state
      |> remove_deferred_waiter_task(issue_id, watcher_token)
      |> poll_deferred_watcher(issue_id, watcher_token)
      |> settle_missing_waiter_receipt(issue_id, watcher_token, result)

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp handle_agent_down(:normal, state, issue_id, running_entry, session_id) do
    cond do
      waiting_watcher?(state, issue_id) ->
        defer_running_issue(state, issue_id, running_entry, session_id)

      Map.get(running_entry, :input_token_warning_status) in ["requested", "delivered"] ->
        hold_exited_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint"
        )

      is_binary(Map.get(running_entry, :resume_phase)) ->
        hold_exited_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint"
        )

      input_required_blocker?(running_entry) ->
        block_input_required_agent_down(state, issue_id, running_entry, session_id, :normal)

      true ->
        Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

        state
        |> complete_issue(issue_id)
        |> schedule_issue_retry(issue_id, 1, %{
          identifier: running_entry.identifier,
          issue_url: running_entry.issue.url,
          delay_type: :continuation,
          worker_host: Map.get(running_entry, :worker_host),
          workspace_path: Map.get(running_entry, :workspace_path)
        })
    end
  end

  defp handle_agent_down(reason, state, issue_id, running_entry, session_id) do
    cond do
      Map.get(running_entry, :input_token_warning_status) in ["requested", "delivered"] ->
        hold_exited_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint_failed"
        )

      is_binary(Map.get(running_entry, :resume_phase)) ->
        hold_exited_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint_failed"
        )

      input_required_blocker?(running_entry) ->
        block_input_required_agent_down(state, issue_id, running_entry, session_id, reason)

      true ->
        retry_agent_down(state, issue_id, running_entry, session_id, reason)
    end
  end

  defp block_input_required_agent_down(state, issue_id, running_entry, session_id, reason) do
    error = blocker_error(running_entry, "agent exited: #{inspect(reason)}")

    Logger.warning("Agent task blocked for issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} session_id=#{session_id}: #{error}")

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp retry_agent_down(state, issue_id, running_entry, session_id, reason) do
    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

    next_attempt = next_retry_attempt_from_running(running_entry)

    schedule_issue_retry(state, issue_id, next_attempt, %{
      identifier: running_entry.identifier,
      issue_url: running_entry.issue.url,
      error: "agent exited: #{inspect(reason)}",
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path)
    })
  end

  defp defer_running_issue(state, issue_id, running_entry, session_id) do
    Logger.info("Agent turn yielded to deferred watcher for issue_id=#{issue_id} session_id=#{session_id}; retaining claim without scheduling a model continuation")

    %{state | deferred: Map.put(state.deferred, issue_id, deferred_metadata(running_entry))}
  end

  @impl true
  def terminate(_reason, %State{waiter_tasks: waiter_tasks, task_supervisor: task_supervisor}) do
    Enum.each(waiter_tasks, fn {_issue_id, task} ->
      stop_waiter_task(task_supervisor, waiter_task_pid(task))
    end)

    :ok
  end

  defp maybe_dispatch(%State{} = state) do
    state =
      state
      |> reconcile_running_issues()
      |> reconcile_blocked_issues()

    with true <- state.hold_store_available,
         :ok <- Config.validate!(),
         {:ok, issues} <- Tracker.fetch_issues_by_states(Config.settings!().tracker.active_states),
         true <- available_slots(state) > 0 do
      choose_issues(issues, state)
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Tracker API token missing in WORKFLOW.md")
        state

      {:error, reason}
      when reason in [
             :missing_linear_intake_scope,
             :conflicting_linear_intake_scope,
             :invalid_linear_project_slug,
             :invalid_linear_team_key
           ] ->
        Logger.error("Tracker intake scope invalid in WORKFLOW.md: #{reason}")
        state

      {:error, :missing_tracker_kind} ->
        Logger.error("Tracker kind missing in WORKFLOW.md")

        state

      {:error, {:unsupported_tracker_kind, kind}} ->
        Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

        state

      {:error, {:invalid_workflow_config, message}} ->
        Logger.error("Invalid WORKFLOW.md config: #{message}")
        state

      {:error, {:missing_workflow_file, path, reason}} ->
        Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
        state

      {:error, :workflow_front_matter_not_a_map} ->
        Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
        state

      {:error, {:workflow_parse_error, reason}} ->
        Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, reason} ->
        Logger.error("Failed to fetch from issue tracker: #{inspect(reason)}")
        state

      false ->
        if state.hold_store_available == false do
          Logger.warning("Skipping issue dispatch while durable hold state is unavailable")
        end

        state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(running_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_running_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_running_issue_ids(running_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  defp reconcile_blocked_issues(%State{} = state) do
    blocked_ids = Map.keys(state.blocked)

    if blocked_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(blocked_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_blocked_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_blocked_issue_ids(blocked_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh blocked issue states: #{inspect(reason)}; keeping blocked issues")

          state
      end
    end
  end

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec reconcile_blocked_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_blocked_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_blocked_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec handle_retry_issue_lookup_for_test(Issue.t(), term(), String.t(), non_neg_integer(), map()) ::
          term()
  def handle_retry_issue_lookup_for_test(%Issue{} = issue, %State{} = state, issue_id, attempt, metadata)
      when is_binary(issue_id) and is_integer(attempt) and attempt >= 0 and is_map(metadata) do
    {:noreply, updated_state} = handle_retry_issue_lookup(issue, state, issue_id, attempt, metadata)
    updated_state
  end

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil) :: String.t() | nil | :no_worker_capacity
  def select_worker_host_for_test(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host)
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, true)

      !issue_routable?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        terminate_running_issue(state, issue.id, false)

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false)
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_blocked_issue_states(
      rest,
      reconcile_blocked_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_blocked_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Blocked issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        cleanup_issue_workspace(issue, Map.get(state.blocked, issue.id, %{}))
        release_issue_claim(state, issue.id)

      !issue_routable?(issue) ->
        Logger.info("Blocked issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; releasing block")
        release_issue_claim(state, issue.id)

      active_issue_state?(issue.state, active_states) ->
        refresh_blocked_issue_state(state, issue)

      true ->
        Logger.info("Blocked issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        release_issue_claim(state, issue.id)
    end
  end

  defp reconcile_blocked_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_missing_running_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        log_missing_running_issue(state_acc, issue_id)
        terminate_running_issue(state_acc, issue_id, false)
      end
    end)
  end

  defp reconcile_missing_running_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp reconcile_missing_blocked_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        Logger.info("Blocked issue no longer visible during state refresh: issue_id=#{issue_id}; releasing block")
        release_issue_claim(state_acc, issue_id)
      end
    end)
  end

  defp reconcile_missing_blocked_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp log_missing_running_issue(%State{} = state, issue_id) when is_binary(issue_id) do
    case Map.get(state.running, issue_id) do
      %{identifier: identifier} ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id} issue_identifier=#{identifier}; stopping active agent")

      _ ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id}; stopping active agent")
    end
  end

  defp log_missing_running_issue(_state, _issue_id), do: :ok

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp refresh_blocked_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.blocked, issue.id) do
      %{issue: _} = blocked_entry ->
        %{state | blocked: Map.put(state.blocked, issue.id, %{blocked_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        state = record_session_completion_totals(state, running_entry)

        stop_running_task(pid, ref, state.task_supervisor)

        if cleanup_workspace do
          cleanup_issue_workspace(Map.get(running_entry, :issue, identifier), running_entry)
        end

        state = clear_pending_resume_hold(state, issue_id)

        %{
          state
          | running: Map.delete(state.running, issue_id),
            claimed: MapSet.delete(state.claimed, issue_id),
            blocked: Map.delete(state.blocked, issue_id),
            retry_attempts: Map.delete(state.retry_attempts, issue_id)
        }

      _ ->
        release_issue_claim(state, issue_id)
    end
  end

  defp enforce_input_token_budget(%State{} = state, issue_id) do
    case Map.get(state.running, issue_id) do
      %{input_token_limit: limit, codex_input_tokens: observed} = running_entry
      when is_integer(limit) and limit > 0 and is_integer(observed) and observed >= limit ->
        hold_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_limit",
          observed
        )

      %{codex_input_tokens: observed} = running_entry when is_integer(observed) ->
        enforce_input_token_budget_below_limit(state, issue_id, running_entry, observed)

      _ ->
        state
    end
  end

  defp enforce_input_token_budget_below_limit(state, issue_id, running_entry, observed) do
    cond do
      Map.get(running_entry, :input_token_warning_status) == "unsupported" ->
        hold_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_warning_unsupported",
          observed
        )

      checkpoint_hold_armed?(state, issue_id) ->
        state

      checkpoint_grace_exhausted?(running_entry, observed) and
          Map.get(running_entry, :input_token_warning_status) == "delivered" ->
        arm_input_token_budget_hold(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint_grace",
          observed
        )

      checkpoint_grace_exhausted?(running_entry, observed) ->
        hold_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "input_token_checkpoint_grace",
          observed
        )

      warning_threshold_reached?(running_entry, observed) ->
        enforce_input_token_warning(state, issue_id, running_entry, observed)

      true ->
        state
    end
  end

  defp checkpoint_grace_exhausted?(running_entry, observed) do
    baseline = Map.get(running_entry, :input_token_warning_observed_at)
    grace = Map.get(running_entry, :input_token_checkpoint_grace)

    is_integer(baseline) and is_integer(grace) and grace > 0 and observed - baseline >= grace
  end

  defp checkpoint_hold_armed?(state, issue_id) do
    match?(
      %{reason: "input_token_checkpoint_grace"},
      Map.get(state.holds, issue_id)
    )
  end

  defp warning_threshold_reached?(running_entry, observed) do
    case Map.get(running_entry, :input_token_limit) do
      limit when is_integer(limit) and limit > 0 ->
        warning_ratio = Map.get(running_entry, :input_token_warning_ratio, 0.70)

        observed >= ceil(limit * warning_ratio) and
          Map.get(running_entry, :input_token_warning_sent, false) == false

      _ ->
        false
    end
  end

  defp enforce_input_token_warning(state, issue_id, running_entry, observed) do
    limit = Map.fetch!(running_entry, :input_token_limit)
    warning_ratio = Map.get(running_entry, :input_token_warning_ratio, 0.70)

    warning_status = steer_input_token_warning(running_entry)

    updated_entry =
      running_entry
      |> Map.put(:input_token_warning_sent, true)
      |> Map.put(:input_token_warning_status, warning_status)
      |> Map.put(:input_token_warning_threshold, ceil(limit * warning_ratio))
      |> Map.put(:input_token_warning_observed_at, observed)

    state = %{state | running: Map.put(state.running, issue_id, updated_entry)}

    if warning_status == "unsupported" do
      hold_input_token_budget_issue(
        state,
        issue_id,
        updated_entry,
        "input_token_warning_unsupported",
        observed
      )
    else
      state
    end
  end

  defp steer_input_token_warning(running_entry) do
    with port when is_port(port) <- Map.get(running_entry, :codex_app_server_port),
         thread_id when is_binary(thread_id) <- Map.get(running_entry, :thread_id),
         turn_id when is_binary(turn_id) <- Map.get(running_entry, :turn_id),
         :ok <- AppServer.steer_turn(port, thread_id, turn_id, token_budget_warning_instruction()) do
      "requested"
    else
      _ -> "unsupported"
    end
  end

  defp token_budget_warning_instruction do
    "Checkpoint only. Finish the already-running atomic tool call, update the persistent workpad with completed work, remaining tasks, validation status, exact HEAD, and stop conditions, then end this turn. Do not start new implementation, review, validation, or waiting work."
  end

  defp settle_input_token_warning_response(running_entry, :token_budget_warning_delivered) do
    Map.put(running_entry, :input_token_warning_status, "delivered")
  end

  defp settle_input_token_warning_response(running_entry, :token_budget_warning_unsupported) do
    Map.put(running_entry, :input_token_warning_status, "unsupported")
  end

  defp settle_input_token_warning_response(running_entry, _event), do: running_entry

  defp hold_input_token_budget_issue(state, issue_id, running_entry, reason, observed) do
    hold = build_input_token_budget_hold(issue_id, running_entry, reason, observed)

    held_state = %{state | holds: Map.put(state.holds, issue_id, hold)}

    case HoldStore.persist(Config.local_workspace_root(), held_state.holds) do
      :ok ->
        held_state
        |> terminate_running_issue(issue_id, false)
        |> Map.update!(:claimed, &MapSet.put(&1, issue_id))

      {:error, persist_reason} ->
        Logger.error("Failed to persist token-budget hold issue_id=#{issue_id} reason=#{inspect(persist_reason)}")

        held_state
        |> Map.put(:hold_store_available, false)
        |> terminate_running_issue(issue_id, false)
        |> Map.update!(:claimed, &MapSet.put(&1, issue_id))
    end
  end

  defp arm_input_token_budget_hold(state, issue_id, running_entry, reason, observed) do
    hold = build_input_token_budget_hold(issue_id, running_entry, reason, observed)

    armed_state = %{
      state
      | holds: Map.put(state.holds, issue_id, hold),
        claimed: MapSet.put(state.claimed, issue_id)
    }

    case HoldStore.persist(Config.local_workspace_root(), armed_state.holds) do
      :ok ->
        armed_state

      {:error, persist_reason} ->
        Logger.error("Failed to persist armed token-budget hold issue_id=#{issue_id} reason=#{inspect(persist_reason)}")

        armed_state
        |> Map.put(:hold_store_available, false)
        |> terminate_running_issue(issue_id, false)
        |> Map.update!(:claimed, &MapSet.put(&1, issue_id))
    end
  end

  defp hold_exited_input_token_budget_issue(state, issue_id, running_entry, reason) do
    observed = Map.get(running_entry, :codex_input_tokens, 0)
    hold = build_input_token_budget_hold(issue_id, running_entry, reason, observed)
    held_state = %{state | holds: Map.put(state.holds, issue_id, hold)}

    case HoldStore.persist(Config.local_workspace_root(), held_state.holds) do
      :ok ->
        %{held_state | claimed: MapSet.put(held_state.claimed, issue_id)}

      {:error, persist_reason} ->
        Logger.error("Failed to persist exited token-budget hold issue_id=#{issue_id} reason=#{inspect(persist_reason)}")

        %{held_state | hold_store_available: false, claimed: MapSet.put(held_state.claimed, issue_id)}
    end
  end

  defp build_input_token_budget_hold(issue_id, running_entry, reason, observed) do
    %{
      issue_id: issue_id,
      identifier: running_entry.identifier,
      reason: reason,
      limit: Map.get(running_entry, :input_token_limit),
      observed_tokens: observed,
      warning_threshold: Map.get(running_entry, :input_token_warning_threshold),
      warning_observed_at: Map.get(running_entry, :input_token_warning_observed_at),
      checkpoint_grace: Map.get(running_entry, :input_token_checkpoint_grace),
      resume_phase: Map.get(running_entry, :resume_phase),
      requested_additional_input_tokens: Map.get(running_entry, :requested_additional_input_tokens),
      effective_additional_input_tokens: Map.get(running_entry, :effective_additional_input_tokens),
      attempt_input_token_baseline: Map.get(running_entry, :attempt_input_token_baseline, 0),
      input_token_tier_limit: Map.get(running_entry, :input_token_tier_limit),
      issue_state: running_entry.issue.state,
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      codex_app_server_pid: Map.get(running_entry, :codex_app_server_pid),
      held_at: DateTime.utc_now(),
      cleanup_pending: false
    }
  end

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.settings!().codex.stall_timeout_ms

    cond do
      timeout_ms <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          maybe_restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp maybe_restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    if Map.has_key?(state.blocked, issue_id) do
      state
    else
      restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if is_integer(elapsed_ms) and elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      cond do
        Map.get(running_entry, :input_token_warning_status) in ["requested", "delivered"] or
            is_binary(Map.get(running_entry, :resume_phase)) ->
          Logger.warning("Bounded issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; re-holding before further work")

          hold_input_token_budget_issue(
            state,
            issue_id,
            running_entry,
            "input_token_checkpoint_failed",
            Map.get(running_entry, :codex_input_tokens, 0)
          )

        input_required_blocker?(running_entry) ->
          error = blocker_error(running_entry, "stalled for #{elapsed_ms}ms after Codex requested operator input")

          Logger.warning("Issue blocked: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; #{error}")

          state
          |> record_session_completion_totals(running_entry)
          |> stop_and_block_issue(issue_id, running_entry, error)

        true ->
          Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

          next_attempt = next_retry_attempt_from_running(running_entry)

          state
          |> terminate_running_issue(issue_id, false)
          |> schedule_issue_retry(issue_id, next_attempt, %{
            identifier: identifier,
            issue_url: running_entry.issue.url,
            error: "stalled for #{elapsed_ms}ms without codex activity"
          })
      end
    else
      state
    end
  end

  defp stall_elapsed_ms(running_entry, now) do
    running_entry
    |> last_activity_timestamp()
    |> case do
      %DateTime{} = timestamp ->
        max(0, DateTime.diff(now, timestamp, :millisecond))

      _ ->
        nil
    end
  end

  defp last_activity_timestamp(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_timestamp) || Map.get(running_entry, :started_at)
  end

  defp last_activity_timestamp(_running_entry), do: nil

  defp input_required_blocker?(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_event) in [:turn_input_required, :approval_required] or
      not is_nil(input_required_completion_outcome(Map.get(running_entry, :completion))) or
      codex_message_method(Map.get(running_entry, :last_codex_message)) ==
        "mcpServer/elicitation/request"
  end

  defp input_required_completion_outcome(completion) when is_map(completion) do
    outcome = Map.get(completion, :outcome) || Map.get(completion, "outcome")
    normalize_input_required_outcome(outcome)
  end

  defp input_required_completion_outcome(_completion), do: nil

  defp normalize_input_required_outcome(outcome)
       when outcome in [:input_required, :needs_input, :approval_required],
       do: outcome

  defp normalize_input_required_outcome(outcome) when is_binary(outcome) do
    case outcome do
      "input_required" -> :input_required
      "needs_input" -> :needs_input
      "approval_required" -> :approval_required
      _ -> nil
    end
  end

  defp normalize_input_required_outcome(_outcome), do: nil

  defp blocker_error(running_entry, fallback) when is_map(running_entry) do
    codex_event_blocker_error(Map.get(running_entry, :last_codex_event)) ||
      completion_blocker_error(Map.get(running_entry, :completion)) ||
      codex_message_blocker_error(Map.get(running_entry, :last_codex_message)) ||
      fallback
  end

  defp codex_event_blocker_error(:turn_input_required), do: "codex turn requires operator input"
  defp codex_event_blocker_error(:approval_required), do: "codex turn requires approval"
  defp codex_event_blocker_error(_event), do: nil

  defp completion_blocker_error(completion) do
    case input_required_completion_outcome(completion) do
      outcome when outcome in [:input_required, :needs_input] -> "codex turn requires operator input"
      :approval_required -> "codex turn requires approval"
      nil -> nil
    end
  end

  defp codex_message_blocker_error(message) do
    if codex_message_method(message) == "mcpServer/elicitation/request" do
      "codex MCP elicitation requires operator input"
    end
  end

  defp codex_message_method(%{message: %{"method" => method}}) when is_binary(method), do: method
  defp codex_message_method(%{message: %{method: method}}) when is_binary(method), do: method
  defp codex_message_method(%{"method" => method}) when is_binary(method), do: method
  defp codex_message_method(%{method: method}) when is_binary(method), do: method
  defp codex_message_method(_message), do: nil

  defp terminate_task(pid, task_supervisor) when is_pid(pid) do
    case Task.Supervisor.terminate_child(task_supervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid, _task_supervisor), do: :ok

  defp stop_running_task(pid, ref, task_supervisor) do
    if is_pid(pid) do
      terminate_task(pid, task_supervisor)
    end

    if is_reference(ref) do
      Process.demonitor(ref, [:flush])
    end

    :ok
  end

  defp stop_and_block_issue(%State{} = state, issue_id, running_entry, error) do
    stop_running_task(
      Map.get(running_entry, :pid),
      Map.get(running_entry, :ref),
      state.task_supervisor
    )

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp block_issue_from_entry(%State{} = state, issue_id, running_entry, error) do
    blocked_entry = %{
      issue_id: issue_id,
      identifier: Map.get(running_entry, :identifier, issue_id),
      issue: Map.get(running_entry, :issue),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      session_id: running_entry_session_id(running_entry),
      error: error,
      blocked_at: DateTime.utc_now(),
      last_codex_message: Map.get(running_entry, :last_codex_message),
      last_codex_event: Map.get(running_entry, :last_codex_event),
      last_codex_timestamp: Map.get(running_entry, :last_codex_timestamp)
    }

    %{
      state
      | running: Map.delete(state.running, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        claimed: MapSet.put(state.claimed, issue_id),
        blocked: Map.put(state.blocked, issue_id, blocked_entry)
    }
  end

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch_issue(state_acc, issue)
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, claimed: claimed, blocked: blocked} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(state.holds, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(blocked, issue.id) and
      available_slots(state) > 0 and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    Enum.all?([id, identifier, title, state_name], &present_string?/1) and
      issue_routable?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    Config.settings!().tracker.terminal_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.settings!().tracker.active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(
         %State{} = state,
         issue,
         attempt \\ nil,
         preferred_worker_host \\ nil,
         phase_budget \\ nil
       ) do
    case refresh_issue_for_dispatch(issue) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(state, refreshed_issue, attempt, preferred_worker_host, phase_budget)

      {:skip, _reason} ->
        state

      {:error, _reason} ->
        state
    end
  end

  defp refresh_issue_for_dispatch(issue) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issues_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        {:ok, refreshed_issue}

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        {:skip, :missing}

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        {:skip, refreshed_issue}

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp do_dispatch_issue(%State{} = state, issue, attempt, preferred_worker_host, phase_budget) do
    recipient = self()

    case select_worker_host(state, preferred_worker_host) do
      :no_worker_capacity ->
        Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
        state

      worker_host ->
        spawn_issue_on_worker_host(state, issue, attempt, recipient, worker_host, phase_budget)
    end
  end

  defp spawn_issue_on_worker_host(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         phase_budget
       ) do
    attempt_session_id =
      24
      |> :crypto.strong_rand_bytes()
      |> Base.url_encode64(padding: false)

    case Task.Supervisor.start_child(state.task_supervisor, fn ->
           state.agent_runner.(issue, recipient,
             attempt: attempt,
             attempt_session_id: attempt_session_id,
             worker_host: worker_host,
             resume_phase: phase_budget_value(phase_budget, :phase),
             max_additional_input_tokens: phase_budget_value(phase_budget, :effective_additional_input_tokens),
             continue_after_turn: fn issue_id ->
               continue_after_turn?(recipient, issue_id)
             end
           )
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")

        running =
          Map.put(state.running, issue.id, %{
            pid: pid,
            ref: ref,
            identifier: issue.identifier,
            issue: issue,
            worker_host: worker_host,
            workspace_path: nil,
            attempt_session_id: attempt_session_id,
            issue_capability_nonces: MapSet.new(),
            session_id: nil,
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
            input_token_limit: phase_budget_limit(phase_budget) || Config.input_token_limit_for_issue(issue),
            input_token_tier_limit: Config.input_token_limit_for_issue(issue),
            input_token_warning_ratio: Config.settings!().codex.input_token_warning_ratio,
            input_token_checkpoint_grace: Config.settings!().codex.input_token_checkpoint_grace,
            input_token_warning_sent: false,
            input_token_warning_status: nil,
            input_token_warning_threshold: nil,
            input_token_warning_observed_at: nil,
            resume_phase: phase_budget_value(phase_budget, :phase),
            requested_additional_input_tokens: phase_budget_value(phase_budget, :requested_additional_input_tokens),
            effective_additional_input_tokens: phase_budget_value(phase_budget, :effective_additional_input_tokens),
            attempt_input_token_baseline: 0,
            turn_count: 0,
            retry_attempt: normalize_retry_attempt(attempt),
            started_at: DateTime.utc_now()
          })

        %{
          state
          | running: running,
            claimed: MapSet.put(state.claimed, issue.id),
            retry_attempts: Map.delete(state.retry_attempts, issue.id)
        }

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

        schedule_issue_retry(state, issue.id, next_attempt, %{
          identifier: issue.identifier,
          issue_url: issue.url,
          error: "failed to spawn agent: #{inspect(reason)}",
          worker_host: worker_host,
          phase_budget: phase_budget
        })
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    if retry_authorized?(state, issue_id, metadata) do
      do_schedule_issue_retry(state, issue_id, attempt, metadata)
    else
      state
    end
  end

  defp retry_authorized?(state, issue_id, metadata) do
    case Map.get(state.holds, issue_id) do
      nil ->
        true

      %{reason: @phase_resume_pending_reason} = hold ->
        phase_budget_matches_hold?(Map.get(metadata, :phase_budget), hold)

      _hold ->
        false
    end
  end

  defp phase_budget_matches_hold?(phase_budget, hold) when is_map(phase_budget) do
    Map.get(phase_budget, :phase) == Map.get(hold, :resume_phase) and
      Map.get(phase_budget, :requested_additional_input_tokens) ==
        Map.get(hold, :requested_additional_input_tokens) and
      Map.get(phase_budget, :effective_additional_input_tokens) ==
        Map.get(hold, :effective_additional_input_tokens)
  end

  defp phase_budget_matches_hold?(_phase_budget, _hold), do: false

  defp do_schedule_issue_retry(%State{} = state, issue_id, attempt, metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    retry_token = make_ref()
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    issue_url = pick_retry_issue_url(previous_retry, metadata)
    error = pick_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)
    phase_budget = Map.get(metadata, :phase_budget, Map.get(previous_retry, :phase_budget))

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms)

    error_suffix = if is_binary(error), do: " error=#{error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    %{
      state
      | retry_attempts:
          Map.put(state.retry_attempts, issue_id, %{
            attempt: next_attempt,
            timer_ref: timer_ref,
            retry_token: retry_token,
            due_at_ms: due_at_ms,
            identifier: identifier,
            issue_url: issue_url,
            error: error,
            worker_host: worker_host,
            workspace_path: workspace_path,
            phase_budget: phase_budget
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          issue_url: Map.get(retry_entry, :issue_url),
          error: Map.get(retry_entry, :error),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path),
          phase_budget: Map.get(retry_entry, :phase_budget)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_issues_by_ids([issue_id]) do
      {:ok, issues} ->
        issues
        |> find_issue_by_id(issue_id)
        |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

      {:error, reason} ->
        Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt + 1,
           Map.merge(metadata, %{error: "retry poll failed: #{inspect(reason)}"})
         )}
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        cleanup_issue_workspace(issue, metadata)
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier, worker_host \\ nil)

  defp cleanup_issue_workspace(issue_or_identifier, metadata) when is_map(metadata) do
    case Map.get(metadata, :workspace_path) do
      workspace_path when is_binary(workspace_path) and workspace_path != "" ->
        Workspace.remove_recorded(
          workspace_path,
          Map.get(metadata, :worker_host),
          issue_or_identifier
        )

      _ ->
        cleanup_issue_workspace(issue_or_identifier, Map.get(metadata, :worker_host))
    end
  end

  defp cleanup_issue_workspace(%Issue{} = issue, worker_host) do
    Workspace.remove_issue_workspaces(issue, worker_host)
  end

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier, worker_host)
  end

  defp cleanup_issue_workspace(_issue_or_identifier, _worker_host), do: :ok

  defp run_terminal_workspace_cleanup do
    case Tracker.fetch_issues_by_states(Config.settings!().tracker.terminal_states) do
      {:ok, issues} ->
        issues
        |> Enum.each(fn
          %Issue{} = issue ->
            cleanup_issue_workspace(issue)

          _ ->
            :ok
        end)

      {:error, reason} ->
        Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
    end
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_authorized?(state, issue.id, metadata) and
         retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host]) do
      case refresh_issue_for_dispatch(issue) do
        {:ok, %Issue{} = refreshed_issue} ->
          {:noreply,
           do_dispatch_issue(
             state,
             refreshed_issue,
             attempt,
             metadata[:worker_host],
             metadata[:phase_budget]
           )}

        {:skip, :missing} ->
          {:noreply, release_issue_claim(state, issue.id)}

        {:skip, %Issue{} = refreshed_issue} ->
          handle_retry_issue_lookup(refreshed_issue, state, issue.id, attempt, metadata)

        {:error, reason} ->
          {:noreply,
           schedule_issue_retry(
             state,
             issue.id,
             attempt + 1,
             Map.merge(metadata, %{
               identifier: issue.identifier,
               error: "retry dispatch refresh failed: #{inspect(reason)}"
             })
           )}
      end
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      {:noreply,
       schedule_issue_retry(
         state,
         issue.id,
         attempt + 1,
         Map.merge(metadata, %{
           identifier: issue.identifier,
           error: "no available orchestrator slots"
         })
       )}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    state = clear_pending_resume_hold(state, issue_id)

    %{
      state
      | claimed: MapSet.delete(state.claimed, issue_id),
        blocked: Map.delete(state.blocked, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  # An `input_token_resume_pending` record is transient bookkeeping for an armed
  # resume; it must not outlive the attempt it authorized. Durable budget holds
  # (checkpoint/grace/limit/manual) are deliberately preserved here — the resume
  # API remains their only exit.
  defp clear_pending_resume_hold(%State{} = state, issue_id) do
    case Map.get(state.holds, issue_id) do
      %{reason: @phase_resume_pending_reason} ->
        cleared = %{state | holds: Map.delete(state.holds, issue_id)}

        case HoldStore.persist(Config.local_workspace_root(), cleared.holds) do
          :ok ->
            Logger.info("Cleared stale armed-resume hold for issue_id=#{issue_id} on attempt conclusion")

            cleared

          {:error, reason} ->
            Logger.warning("Failed to persist cleared armed-resume hold for issue_id=#{issue_id}: #{inspect(reason)}; keeping hold")

            state
        end

      _ ->
        state
    end
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    if metadata[:delay_type] == :continuation and attempt == 1 do
      @continuation_retry_delay_ms
    else
      failure_retry_delay(attempt)
    end
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.settings!().agent.max_retry_backoff_ms)
  end

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp phase_budget_limit(phase_budget) when is_map(phase_budget),
    do: Map.get(phase_budget, :effective_additional_input_tokens)

  defp phase_budget_limit(_phase_budget), do: nil

  defp phase_budget_value(phase_budget, key) when is_map(phase_budget),
    do: Map.get(phase_budget, key)

  defp phase_budget_value(_phase_budget, _key), do: nil

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_issue_url(previous_retry, metadata) do
    metadata[:issue_url] || Map.get(previous_retry, :issue_url)
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp pick_retry_worker_host(previous_retry, metadata) do
    metadata[:worker_host] || Map.get(previous_retry, :worker_host)
  end

  defp pick_retry_workspace_path(previous_retry, metadata) do
    metadata[:workspace_path] || Map.get(previous_retry, :workspace_path)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp select_worker_host(%State{} = state, preferred_worker_host) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        nil

      hosts ->
        available_hosts = Enum.filter(hosts, &worker_host_slots_available?(state, &1))

        cond do
          available_hosts == [] ->
            :no_worker_capacity

          preferred_worker_host_available?(preferred_worker_host, available_hosts) ->
            preferred_worker_host

          true ->
            least_loaded_worker_host(state, available_hosts)
        end
    end
  end

  defp preferred_worker_host_available?(preferred_worker_host, hosts)
       when is_binary(preferred_worker_host) and is_list(hosts) do
    preferred_worker_host != "" and preferred_worker_host in hosts
  end

  defp preferred_worker_host_available?(_preferred_worker_host, _hosts), do: false

  defp least_loaded_worker_host(%State{} = state, hosts) when is_list(hosts) do
    hosts
    |> Enum.with_index()
    |> Enum.min_by(fn {host, index} ->
      {running_worker_host_count(state.running, host), index}
    end)
    |> elem(0)
  end

  defp running_worker_host_count(running, worker_host) when is_map(running) and is_binary(worker_host) do
    Enum.count(running, fn
      {_issue_id, %{worker_host: ^worker_host}} -> true
      _ -> false
    end)
  end

  defp worker_slots_available?(%State{} = state) do
    select_worker_host(state, nil) != :no_worker_capacity
  end

  defp worker_slots_available?(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host) != :no_worker_capacity
  end

  defp worker_host_slots_available?(%State{} = state, worker_host) when is_binary(worker_host) do
    case Config.settings!().worker.max_concurrent_agents_per_host do
      limit when is_integer(limit) and limit > 0 ->
        running_worker_host_count(state.running, worker_host) < limit

      _ ->
        true
    end
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    max(
      (state.max_concurrent_agents || Config.settings!().agent.max_concurrent_agents) -
        map_size(state.running),
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec resume_issue(String.t()) :: {:ok, map()} | {:error, atom()}
  def resume_issue(issue_identifier), do: resume_issue(issue_identifier, %{}, __MODULE__)

  @spec stop_issue(String.t()) :: {:ok, map()} | {:error, atom()}
  def stop_issue(issue_identifier), do: stop_issue(issue_identifier, __MODULE__)

  @spec stop_issue(String.t(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def stop_issue(issue_identifier, server) when is_binary(issue_identifier) do
    GenServer.call(server, {:stop_issue, issue_identifier})
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec resume_issue(String.t(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def resume_issue(issue_identifier, server) when is_binary(issue_identifier) do
    resume_issue(issue_identifier, %{}, server)
  end

  @spec resume_issue(String.t(), map(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def resume_issue(issue_identifier, options, server)
      when is_binary(issue_identifier) and is_map(options) do
    GenServer.call(server, {:resume_issue, issue_identifier, options})
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec record_progress(String.t(), map(), GenServer.server()) ::
          {:ok, map()} | {:error, atom()}
  def record_progress(issue_identifier, attributes, server \\ __MODULE__)
      when is_binary(issue_identifier) and is_map(attributes) do
    GenServer.call(server, {:record_progress, issue_identifier, attributes})
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec authorize_review(String.t(), map(), GenServer.server()) ::
          {:ok, map()} | {:error, atom()}
  def authorize_review(issue_identifier, attributes, server \\ __MODULE__)
      when is_binary(issue_identifier) and is_map(attributes) do
    GenServer.call(server, {:authorize_review, issue_identifier, attributes})
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec register_deferred_wait(String.t(), map(), GenServer.server()) ::
          {:error, :deferred_wait_disabled}
  def register_deferred_wait(_issue_identifier, _attributes, _server \\ __MODULE__),
    do: {:error, :deferred_wait_disabled}

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          issue_url: metadata.issue.url,
          state: metadata.issue.state,
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          input_token_warning_status: Map.get(metadata, :input_token_warning_status),
          input_token_warning_threshold: Map.get(metadata, :input_token_warning_threshold),
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
        |> Map.merge(progress_metadata(state, issue_id))
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          issue_url: Map.get(retry, :issue_url),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path)
        }
      end)

    blocked =
      state.blocked
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: Map.get(metadata, :identifier),
          issue_url: blocked_issue_url(metadata),
          state: blocked_issue_state(metadata),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: Map.get(metadata, :session_id),
          error: Map.get(metadata, :error),
          blocked_at: Map.get(metadata, :blocked_at),
          last_codex_timestamp: Map.get(metadata, :last_codex_timestamp),
          last_codex_message: Map.get(metadata, :last_codex_message),
          last_codex_event: Map.get(metadata, :last_codex_event)
        }
      end)

    held =
      state.holds
      |> Map.values()
      |> Enum.sort_by(& &1.identifier)

    deferred =
      Enum.map(state.deferred, fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          state: "deferred_wait",
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          started_at: Map.get(metadata, :started_at)
        }
      end)

    {:reply,
     %{
       running: running,
       deferred: deferred,
       retrying: retrying,
       blocked: blocked,
       held: held,
       codex_totals: state.codex_totals,
       rate_limits: Map.get(state, :codex_rate_limits),
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call({:resume_issue, issue_identifier, options}, _from, state) do
    case find_hold_by_identifier(state.holds, issue_identifier) do
      nil ->
        {:reply, {:error, :issue_not_found}, state}

      {issue_id, hold} ->
        authorize_resume(state, issue_id, hold, options)
    end
  end

  def handle_call({:stop_issue, issue_identifier}, _from, state) do
    case find_known_issue_id(state, issue_identifier) do
      nil ->
        {:reply, {:error, :issue_not_found}, state}

      issue_id ->
        case stop_known_issue(state, issue_id) do
          {:ok, updated_state, hold} -> {:reply, {:ok, hold}, updated_state}
          {:error, updated_state, reason} -> {:reply, {:error, reason}, updated_state}
        end
    end
  end

  def handle_call({:record_progress, issue_identifier, attributes}, _from, state) do
    case record_progress_state(state, issue_identifier, attributes) do
      {:ok, updated_state, receipt} -> {:reply, {:ok, receipt}, updated_state}
      {:error, reason, updated_state} -> {:reply, {:error, reason}, updated_state}
    end
  end

  def handle_call({:authorize_review, issue_identifier, attributes}, _from, state) do
    case authorize_review_state(state, issue_identifier, attributes) do
      {:ok, updated_state, receipt} -> {:reply, {:ok, receipt}, updated_state}
      {:error, reason, updated_state} -> {:reply, {:error, reason}, updated_state}
    end
  end

  def handle_call({:register_deferred_wait, _issue_identifier, _attributes}, _from, state),
    do: {:reply, {:error, :deferred_wait_disabled}, state}

  def handle_call({:continue_after_turn, issue_id}, _from, state) do
    continue? =
      not waiting_watcher?(state, issue_id) and
        not checkpoint_hold_armed?(state, issue_id)

    {:reply, continue?, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?
    state = if coalesced, do: state, else: schedule_tick(state, 0)

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  defp find_known_issue_id(%State{} = state, issue_identifier) do
    find_issue_id_by_identifier(state.running, issue_identifier) ||
      find_issue_id_by_identifier(state.deferred, issue_identifier) ||
      find_issue_id_by_identifier(state.retry_attempts, issue_identifier) ||
      find_issue_id_by_identifier(state.holds, issue_identifier) ||
      find_issue_id_by_identifier(state.blocked, issue_identifier)
  end

  defp find_hold_by_identifier(holds, issue_identifier) do
    Enum.find(holds, fn {_issue_id, hold} ->
      same_identifier?(Map.get(hold, :identifier), issue_identifier)
    end)
  end

  defp find_issue_id_by_identifier(entries, issue_identifier) do
    Enum.find_value(entries, fn {issue_id, entry} ->
      if same_identifier?(entry_identifier(entry), issue_identifier), do: issue_id
    end)
  end

  defp entry_identifier(entry) when is_map(entry) do
    Map.get(entry, :identifier, Map.get(entry, "identifier"))
  end

  defp same_identifier?(left, right) when is_binary(left) and is_binary(right) do
    String.downcase(left) == String.downcase(right)
  end

  defp same_identifier?(_left, _right), do: false

  defp stop_known_issue(%State{} = state, issue_id) do
    cond do
      running_entry = Map.get(state.running, issue_id) ->
        stop_running_issue(state, issue_id, running_entry)

      deferred_entry = Map.get(state.deferred, issue_id) ->
        stop_deferred_issue(state, issue_id, deferred_entry)

      retry_entry = Map.get(state.retry_attempts, issue_id) ->
        stop_retrying_issue(state, issue_id, retry_entry)

      hold = Map.get(state.holds, issue_id) ->
        {:ok, state, hold}

      true ->
        {:error, state, :issue_not_found}
    end
  end

  defp stop_deferred_issue(state, issue_id, deferred_entry) do
    progress = Map.fetch!(state.progress, issue_id)

    cancelled_watcher =
      progress
      |> Map.fetch!("watcher")
      |> Map.put("state", "cancelled")
      |> Map.put("completed_at", DateTime.utc_now() |> DateTime.to_iso8601())

    cancelled_progress =
      progress
      |> Map.put("watcher", cancelled_watcher)
      |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())

    persisted_state =
      persist_progress_map(state, Map.put(state.progress, issue_id, cancelled_progress), issue_id)

    if persisted_state.progress_state_available do
      stopped_state =
        persisted_state
        |> stop_deferred_waiter_task(issue_id)
        |> Map.update!(:deferred, &Map.delete(&1, issue_id))

      stop_retrying_issue(stopped_state, issue_id, deferred_entry)
    else
      {:error, persisted_state, :hold_state_unavailable}
    end
  end

  defp stop_running_issue(%State{hold_store_available: false} = state, _issue_id, _running_entry) do
    {:error, state, :hold_state_unavailable}
  end

  defp stop_running_issue(state, issue_id, running_entry) do
    pending_hold =
      issue_id
      |> build_input_token_budget_hold(
        running_entry,
        "manual_stop",
        Map.get(running_entry, :codex_input_tokens, 0)
      )
      |> Map.merge(%{limit: nil, cleanup_pending: true})

    held_state = %{
      state
      | holds: Map.put(state.holds, issue_id, pending_hold),
        claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }

    case persist_holds(held_state) do
      :ok ->
        finish_running_manual_stop(held_state, issue_id, pending_hold)

      {:error, _reason} ->
        unavailable_state =
          held_state
          |> Map.put(:hold_store_available, false)
          |> terminate_running_issue(issue_id, false)
          |> Map.update!(:claimed, &MapSet.put(&1, issue_id))

        {:error, unavailable_state, :hold_state_unavailable}
    end
  end

  defp finish_running_manual_stop(held_state, issue_id, pending_hold) do
    stopped_state =
      held_state
      |> terminate_running_issue(issue_id, false)
      |> Map.update!(:claimed, &MapSet.put(&1, issue_id))

    hold = Map.put(pending_hold, :cleanup_pending, false)
    cleaned_state = %{stopped_state | holds: Map.put(stopped_state.holds, issue_id, hold)}

    case persist_holds(cleaned_state) do
      :ok ->
        {:ok, cleaned_state, hold}

      {:error, _reason} ->
        unavailable_state = %{
          stopped_state
          | holds: Map.put(stopped_state.holds, issue_id, pending_hold),
            hold_store_available: false
        }

        {:error, unavailable_state, :hold_state_unavailable}
    end
  end

  defp stop_retrying_issue(%State{hold_store_available: false} = state, _issue_id, _retry_entry) do
    {:error, state, :hold_state_unavailable}
  end

  defp stop_retrying_issue(state, issue_id, retry_entry) do
    hold = %{
      issue_id: issue_id,
      identifier: retry_entry.identifier,
      reason: "manual_stop",
      limit: nil,
      observed_tokens: 0,
      issue_state: Map.get(retry_entry, :issue_state),
      worker_host: Map.get(retry_entry, :worker_host),
      workspace_path: Map.get(retry_entry, :workspace_path),
      codex_app_server_pid: nil,
      cleanup_pending: false,
      held_at: DateTime.utc_now()
    }

    held_state = %{
      state
      | holds: Map.put(state.holds, issue_id, hold),
        claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }

    case persist_holds(held_state) do
      :ok ->
        cancel_retry_timer(retry_entry)
        {:ok, held_state, hold}

      {:error, _reason} ->
        {:error, state, :hold_state_unavailable}
    end
  end

  defp persist_holds(%State{} = state) do
    HoldStore.persist(Config.local_workspace_root(), state.holds)
  end

  defp cancel_retry_timer(retry_entry) do
    case Map.get(retry_entry, :timer_ref) do
      timer_ref when is_reference(timer_ref) -> Process.cancel_timer(timer_ref)
      _ -> :ok
    end
  end

  defp authorize_resume(
         %State{progress_state_available: false} = state,
         _issue_id,
         _hold,
         _options
       ) do
    {:reply, {:error, :progress_state_unavailable}, state}
  end

  defp authorize_resume(%State{running: running} = state, issue_id, _hold, _options)
       when is_map_key(running, issue_id) do
    {:reply, {:error, :issue_running}, state}
  end

  defp authorize_resume(state, issue_id, hold, options) do
    with {:ok, phase} <- validate_resume_phase(option_value(options, :phase)),
         {:ok, requested} <-
           validate_resume_allowance(option_value(options, :max_additional_input_tokens)),
         {:ok, tier_limit} <- current_issue_tier_limit(issue_id) do
      phase_budget = build_phase_budget(phase, requested, tier_limit)
      persist_resume_authorization(state, issue_id, hold, phase_budget)
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp build_phase_budget(phase, requested, tier_limit) do
    effective = if is_integer(tier_limit), do: min(requested, tier_limit), else: requested

    %{
      phase: phase,
      requested_additional_input_tokens: requested,
      effective_additional_input_tokens: effective,
      attempt_input_token_baseline: 0,
      current_issue_tier_limit: tier_limit
    }
  end

  defp validate_resume_phase(nil), do: {:error, :resume_phase_required}

  defp validate_resume_phase(phase) when is_binary(phase) do
    normalized = phase |> String.trim() |> String.downcase()

    if normalized in @resume_phases do
      {:ok, normalized}
    else
      {:error, :invalid_resume_phase}
    end
  end

  defp validate_resume_phase(_phase), do: {:error, :invalid_resume_phase}

  defp validate_resume_allowance(nil),
    do: {:error, :max_additional_input_tokens_required}

  defp validate_resume_allowance(value) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp validate_resume_allowance(_value),
    do: {:error, :invalid_max_additional_input_tokens}

  defp current_issue_tier_limit(issue_id) do
    case Tracker.fetch_issues_by_ids([issue_id]) do
      {:ok, [%Issue{} = issue | _]} -> {:ok, Config.input_token_limit_for_issue(issue)}
      _ -> {:error, :tracker_unavailable}
    end
  end

  defp option_value(options, key) do
    Map.get(options, key, Map.get(options, Atom.to_string(key)))
  end

  defp persist_resume_authorization(state, issue_id, hold, phase_budget) do
    pending_hold =
      Map.merge(hold, %{
        reason: @phase_resume_pending_reason,
        limit: phase_budget.effective_additional_input_tokens,
        observed_tokens: 0,
        warning_threshold: nil,
        warning_observed_at: nil,
        checkpoint_grace: Config.settings!().codex.input_token_checkpoint_grace,
        resume_phase: phase_budget.phase,
        requested_additional_input_tokens: phase_budget.requested_additional_input_tokens,
        effective_additional_input_tokens: phase_budget.effective_additional_input_tokens,
        attempt_input_token_baseline: 0,
        input_token_tier_limit: phase_budget.current_issue_tier_limit,
        codex_app_server_pid: nil,
        cleanup_pending: false,
        held_at: DateTime.utc_now()
      })

    authorized_state = %{
      state
      | holds: Map.put(state.holds, issue_id, pending_hold),
        claimed: MapSet.put(state.claimed, issue_id)
    }

    case HoldStore.persist(Config.local_workspace_root(), authorized_state.holds) do
      :ok -> finish_phase_resume(authorized_state, issue_id, pending_hold, phase_budget)
      {:error, _reason} -> {:reply, {:error, :hold_state_unavailable}, state}
    end
  end

  defp finish_phase_resume(state, issue_id, hold, phase_budget) do
    updated_state =
      schedule_issue_retry(state, issue_id, 1, %{
        identifier: hold.identifier,
        worker_host: Map.get(hold, :worker_host),
        workspace_path: Map.get(hold, :workspace_path),
        delay_type: :continuation,
        phase_budget: phase_budget
      })

    receipt =
      %{
        issue_id: issue_id,
        identifier: hold.identifier,
        resumed: true,
        workspace_path: Map.get(hold, :workspace_path)
      }
      |> Map.merge(phase_budget)

    {:reply, {:ok, receipt}, updated_state}
  end

  defp record_progress_state(
         %State{progress_state_available: false} = state,
         _issue_identifier,
         _attributes
       ) do
    {:error, :progress_state_unavailable, state}
  end

  defp record_progress_state(state, issue_identifier, attributes) do
    case require_active_owner_session(state, issue_identifier, attributes) do
      {:ok, issue_id} ->
        case consume_issue_request_nonce(state, issue_id, attributes) do
          {:ok, authorized_state} ->
            record_authorized_progress(authorized_state, issue_id, issue_identifier, attributes)

          {:error, reason} ->
            {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp record_authorized_progress(state, issue_id, issue_identifier, attributes) do
    with {:ok, fingerprint} <-
           normalize_progress_fingerprint(option_value(attributes, :fingerprint)),
         {:ok, progress_kind} <-
           normalize_progress_kind(option_value(attributes, :progress_kind)),
         {:ok, progress_receipt} <-
           normalize_progress_receipt(option_value(attributes, :progress_receipt)) do
      existing =
        Map.get(state.progress, issue_id) ||
          new_progress_entry(state, issue_id, issue_identifier)

      record_progress_entry(
        state,
        issue_id,
        existing,
        fingerprint,
        progress_kind,
        progress_receipt
      )
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp record_progress_entry(
         state,
         issue_id,
         existing,
         fingerprint,
         progress_kind,
         progress_receipt
       ) do
    progress_hash = canonical_hash(fingerprint)
    review_hash = canonical_hash(Map.take(fingerprint, @review_fingerprint_fields))

    if progress_hash == Map.get(existing, "progress_fingerprint_hash") and
         progress_receipt == Map.get(existing, "last_progress_receipt") do
      {:ok, state,
       %{
         changed: false,
         progress_fingerprint: progress_hash,
         review_fingerprint: review_hash
       }}
    else
      persist_progress_change(
        state,
        issue_id,
        existing,
        fingerprint,
        progress_kind,
        progress_receipt,
        progress_hash,
        review_hash
      )
    end
  end

  defp persist_progress_change(
         state,
         issue_id,
         existing,
         fingerprint,
         progress_kind,
         progress_receipt,
         progress_hash,
         review_hash
       ) do
    with :ok <- validate_review_receipt_authorization(existing, progress_kind, fingerprint),
         {:ok, updated} <-
           existing
           |> update_review_identity(fingerprint, review_hash)
           |> complete_review_receipt(progress_kind, progress_receipt) do
      updated =
        Map.merge(updated, %{
          "fingerprint" => fingerprint,
          "progress_fingerprint_hash" => progress_hash,
          "review_fingerprint_hash" => review_hash,
          "last_progress_kind" => progress_kind,
          "last_progress_receipt" => progress_receipt,
          "updated_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      persist_progress_entry(state, issue_id, updated, %{
        changed: true,
        progress_fingerprint: progress_hash,
        review_fingerprint: review_hash
      })
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp authorize_review_state(
         %State{progress_state_available: false} = state,
         _issue_identifier,
         _attributes
       ) do
    {:error, :progress_state_unavailable, state}
  end

  defp authorize_review_state(state, issue_identifier, attributes) do
    case require_active_owner_session(state, issue_identifier, attributes) do
      {:ok, issue_id} ->
        case consume_issue_request_nonce(state, issue_id, attributes) do
          {:ok, authorized_state} ->
            authorize_consumed_review(authorized_state, issue_id, attributes)

          {:error, reason} ->
            {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp authorize_consumed_review(state, issue_id, attributes) do
    with %{} = progress <- Map.get(state.progress, issue_id),
         {:ok, kind} <- normalize_review_kind(option_value(attributes, :kind)),
         {:ok, requested_fingerprint} <-
           requested_review_fingerprint(attributes, progress),
         true <- requested_fingerprint == Map.get(progress, "review_fingerprint_hash"),
         {:ok, requested_head} <-
           normalize_commit_sha(option_value(attributes, :requested_head)),
         true <- fingerprint_contains_head?(Map.get(progress, "fingerprint", %{}), requested_head),
         {:ok, ^requested_head} <-
           normalize_commit_sha(option_value(attributes, :observed_local_head)),
         {:ok, ^requested_head} <-
           normalize_commit_sha(option_value(attributes, :observed_remote_head)),
         {:ok, _eligible} <-
           increment_review_round(progress, kind, option_value(attributes, :human_override)) do
      authorization =
        canonical_hash(%{
          issue_id: issue_id,
          kind: kind,
          review_fingerprint: requested_fingerprint,
          requested_head: requested_head,
          round: Map.get(progress, "review_round_count", 0) + 1,
          issued_at: System.unique_integer([:positive, :monotonic])
        })

      updated =
        progress
        |> Map.put("last_review_authorization", authorization)
        |> Map.put("last_review_authorized_head", requested_head)
        |> Map.put("last_review_authorized_fingerprint", requested_fingerprint)
        |> Map.put("last_review_authorized_kind", kind)
        |> Map.put("last_review_authorized_override", option_value(attributes, :human_override))
        |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())

      persist_progress_entry(state, issue_id, updated, %{
        authorized: true,
        authorization: authorization,
        kind: kind,
        review_round_count: Map.get(updated, "review_round_count", 0),
        security_review_count: Map.get(updated, "security_review_count", 0),
        review_fingerprint: requested_fingerprint,
        requested_head: requested_head
      })
    else
      nil -> {:error, :issue_not_found, state}
      false -> {:error, :review_fingerprint_mismatch, state}
      {:error, reason} -> {:error, reason, state}
      _ -> {:error, :stale_review_head, state}
    end
  end

  defp require_active_owner_session(state, issue_identifier, attributes) do
    case find_known_issue_id(state, issue_identifier) do
      nil ->
        {:error, :issue_not_found}

      issue_id ->
        if option_value(attributes, :owner_session_authorized) == true,
          do: {:ok, issue_id},
          else: require_running_owner_session(state, issue_id, attributes)
    end
  end

  defp require_running_owner_session(state, issue_id, attributes) do
    case Map.get(state.running, issue_id) do
      %{attempt_session_id: current_session} when is_binary(current_session) ->
        if option_value(attributes, :owner_session) == current_session,
          do: {:ok, issue_id},
          else: {:error, :stale_issue_session}

      _ ->
        {:error, :issue_not_running}
    end
  end

  defp consume_issue_request_nonce(state, issue_id, attributes) do
    if option_value(attributes, :issue_request_authorized) == true do
      running_entry = Map.fetch!(state.running, issue_id)
      nonce = option_value(attributes, :issue_capability_nonce)
      seen_nonces = Map.get(running_entry, :issue_capability_nonces, MapSet.new())

      cond do
        not is_binary(nonce) or nonce == "" ->
          {:error, :invalid_issue_capability}

        MapSet.member?(seen_nonces, nonce) ->
          {:error, :replayed_issue_request}

        MapSet.size(seen_nonces) >= @max_issue_request_nonces ->
          {:error, :issue_nonce_capacity_exceeded}

        true ->
          updated_entry =
            Map.put(running_entry, :issue_capability_nonces, MapSet.put(seen_nonces, nonce))

          {:ok, %{state | running: Map.put(state.running, issue_id, updated_entry)}}
      end
    else
      {:ok, state}
    end
  end

  defp requested_review_fingerprint(attributes, progress) do
    case option_value(attributes, :review_fingerprint) do
      value when is_binary(value) ->
        {:ok, value}

      _ ->
        if option_value(attributes, :owner_session_authorized) == true,
          do: {:error, :review_fingerprint_required},
          else: scoped_review_fingerprint(progress)
    end
  end

  defp scoped_review_fingerprint(progress) do
    case Map.get(progress, "review_fingerprint_hash") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :review_fingerprint_required}
    end
  end

  defp normalize_progress_fingerprint(fingerprint) when is_map(fingerprint) do
    normalized =
      Enum.reduce(@progress_fingerprint_fields, %{}, fn field, acc ->
        Map.put(acc, field, Map.get(fingerprint, field, Map.get(fingerprint, String.to_atom(field))))
      end)

    with :ok <- validate_contract_revision(normalized),
         :ok <- validate_progress_heads(normalized),
         :ok <- validate_progress_checksums(normalized),
         {:ok, required_checks} <- normalize_required_check_set(normalized) do
      normalized =
        normalized
        |> Map.update!("diff_checksum", &normalize_checksum_container/1)
        |> Map.update!("matrix_checksum", &normalize_checksum_container/1)
        |> Map.put("required_check_set", required_checks)

      {:ok, normalized}
    end
  end

  defp normalize_progress_fingerprint(_fingerprint),
    do: {:error, :incomplete_progress_fingerprint}

  defp validate_contract_revision(%{"contract_revision" => revision})
       when is_binary(revision) do
    if String.trim(revision) == "", do: {:error, :incomplete_progress_fingerprint}, else: :ok
  end

  defp validate_contract_revision(_fingerprint),
    do: {:error, :incomplete_progress_fingerprint}

  defp validate_progress_heads(fingerprint) do
    if valid_sha_container?(fingerprint["base_sha"]) and
         valid_sha_container?(fingerprint["head_sha"]) do
      :ok
    else
      {:error, :invalid_progress_head}
    end
  end

  defp validate_progress_checksums(fingerprint) do
    if valid_checksum_container?(fingerprint["diff_checksum"]) and
         valid_checksum_container?(fingerprint["matrix_checksum"]) do
      :ok
    else
      {:error, :invalid_progress_checksum}
    end
  end

  defp normalize_required_check_set(%{"required_check_set" => checks}) when is_list(checks) do
    if Enum.all?(checks, &(is_binary(&1) and String.trim(&1) != "")) do
      {:ok, checks |> Enum.uniq() |> Enum.sort()}
    else
      {:error, :invalid_required_check_set}
    end
  end

  defp normalize_required_check_set(_fingerprint), do: {:error, :invalid_required_check_set}

  defp normalize_progress_kind(kind)
       when kind in [
              "git",
              "workpad_checkpoint",
              "validation_receipt",
              "review_receipt",
              "human_direction",
              "provider_transition"
            ],
       do: {:ok, kind}

  defp normalize_progress_kind(kind) when is_atom(kind),
    do: normalize_progress_kind(Atom.to_string(kind))

  defp normalize_progress_kind(_kind), do: {:error, :invalid_progress_kind}

  defp normalize_progress_receipt(receipt) when is_binary(receipt) do
    if String.trim(receipt) == "", do: {:error, :invalid_progress_receipt}, else: {:ok, receipt}
  end

  defp normalize_progress_receipt(_receipt), do: {:error, :invalid_progress_receipt}

  defp normalize_review_kind(kind) when kind in @review_kinds, do: {:ok, kind}
  defp normalize_review_kind(kind) when is_atom(kind), do: normalize_review_kind(Atom.to_string(kind))
  defp normalize_review_kind(_kind), do: {:error, :invalid_review_kind}

  defp increment_review_round(progress, kind, override) do
    with :ok <- validate_review_override(progress, override) do
      do_increment_review_round(progress, kind, override)
    end
  end

  defp do_increment_review_round(progress, "full", override) do
    if Map.get(progress, "full_review_count", 0) == 0 or valid_human_override?(override) do
      {:ok,
       progress
       |> Map.update("full_review_count", 1, &(&1 + 1))
       |> Map.update("review_round_count", 1, &(&1 + 1))
       |> maybe_record_review_override(override)}
    else
      {:error, :full_review_already_completed}
    end
  end

  defp do_increment_review_round(progress, "delta", override) do
    cond do
      Map.get(progress, "full_review_count", 0) == 0 ->
        {:error, :full_review_required}

      Map.get(progress, "delta_review_count", 0) == 0 or valid_human_override?(override) ->
        {:ok,
         progress
         |> Map.update("delta_review_count", 1, &(&1 + 1))
         |> Map.update("review_round_count", 1, &(&1 + 1))
         |> maybe_record_review_override(override)}

      true ->
        {:error, :delta_review_already_completed}
    end
  end

  defp do_increment_review_round(progress, "security", override) do
    if Map.get(progress, "security_review_count", 0) == 0 or valid_human_override?(override) do
      {:ok,
       progress
       |> Map.update("security_review_count", 1, &(&1 + 1))
       |> maybe_record_review_override(override)}
    else
      {:error, :security_review_already_completed}
    end
  end

  defp valid_human_override?(override),
    do: is_binary(override) and Regex.match?(@human_override_pattern, override)

  defp validate_review_override(progress, override) do
    case Map.get(progress, "used_review_overrides", []) do
      overrides when is_list(overrides) ->
        if valid_human_override?(override) and override in overrides do
          {:error, :review_override_already_used}
        else
          :ok
        end

      _invalid ->
        {:error, :review_override_state_invalid}
    end
  end

  defp maybe_record_review_override(progress, override) do
    if valid_human_override?(override) do
      progress
      |> Map.put("review_override", override)
      |> Map.update("used_review_overrides", [override], fn overrides ->
        [override | overrides] |> Enum.uniq()
      end)
    else
      progress
    end
  end

  defp validate_review_receipt_authorization(progress, "review_receipt", fingerprint) do
    authorized_head = Map.get(progress, "last_review_authorized_head")
    authorized_fingerprint = Map.get(progress, "last_review_authorized_fingerprint")
    receipt_fingerprint = canonical_hash(Map.take(fingerprint, @review_fingerprint_fields))

    if is_binary(authorized_head) and is_binary(authorized_fingerprint) and
         fingerprint_contains_head?(fingerprint, authorized_head) and
         receipt_fingerprint == authorized_fingerprint do
      :ok
    else
      {:error, :review_authorization_mismatch}
    end
  end

  defp validate_review_receipt_authorization(_progress, _kind, _fingerprint), do: :ok

  defp complete_review_receipt(progress, "review_receipt", receipt) do
    authorization = Map.get(progress, "last_review_authorization")
    kind = Map.get(progress, "last_review_authorized_kind")
    override = Map.get(progress, "last_review_authorized_override")

    with true <- is_binary(authorization) and kind in @review_kinds,
         {:ok, completed} <- increment_review_round(progress, kind, override) do
      {:ok,
       completed
       |> Map.put("last_completed_review_authorization", authorization)
       |> Map.put("last_review_receipt", receipt)
       |> Map.delete("last_review_authorized_head")
       |> Map.delete("last_review_authorized_fingerprint")
       |> Map.delete("last_review_authorized_kind")
       |> Map.delete("last_review_authorized_override")}
    else
      false -> {:error, :review_authorization_mismatch}
      {:error, reason} -> {:error, reason}
    end
  end

  defp complete_review_receipt(progress, _kind, _receipt), do: {:ok, progress}

  defp update_review_identity(existing, fingerprint, review_hash) do
    prior_fingerprint = Map.get(existing, "fingerprint", %{})
    prior_review_hash = Map.get(existing, "review_fingerprint_hash")

    cond do
      is_nil(prior_review_hash) ->
        initialize_review_counts(existing)

      prior_review_hash == review_hash ->
        existing

      contract_or_matrix_changed?(prior_fingerprint, fingerprint) ->
        reset_review_counts(existing, prior_fingerprint, fingerprint)

      Map.get(existing, "full_review_count", 0) > 0 ->
        existing
        |> Map.put("delta_review_count", 0)
        |> Map.put("review_round_count", Map.get(existing, "full_review_count", 0))
        |> Map.put("security_review_count", 0)

      true ->
        initialize_review_counts(existing)
    end
  end

  defp contract_or_matrix_changed?(prior, current) do
    Map.get(prior, "contract_revision") != Map.get(current, "contract_revision") or
      normalize_checksum_container(Map.get(prior, "matrix_checksum")) !=
        normalize_checksum_container(Map.get(current, "matrix_checksum"))
  end

  defp reset_review_counts(entry, prior_fingerprint, current_fingerprint) do
    Logger.info(
      "Review counts reset issue_id=#{Map.get(entry, "issue_id")} issue_identifier=#{Map.get(entry, "identifier")} reason=contract_or_matrix_changed " <>
        "prior_contract_revision=#{inspect(Map.get(prior_fingerprint, "contract_revision"))} " <>
        "current_contract_revision=#{inspect(Map.get(current_fingerprint, "contract_revision"))} " <>
        "prior_matrix_checksum=#{inspect(Map.get(prior_fingerprint, "matrix_checksum"))} " <>
        "current_matrix_checksum=#{inspect(Map.get(current_fingerprint, "matrix_checksum"))}"
    )

    initialize_review_counts(entry)
  end

  defp initialize_review_counts(entry) do
    entry
    |> Map.put("full_review_count", 0)
    |> Map.put("delta_review_count", 0)
    |> Map.put("security_review_count", 0)
    |> Map.put("review_round_count", 0)
    |> Map.delete("review_override")
  end

  defp new_progress_entry(state, issue_id, issue_identifier) do
    workspace_path =
      state.running
      |> Map.get(issue_id, %{})
      |> Map.get(:workspace_path)

    %{
      "issue_id" => issue_id,
      "identifier" => issue_identifier,
      "workspace_path" => workspace_path,
      "fingerprint" => %{},
      "review_round_count" => 0,
      "full_review_count" => 0,
      "delta_review_count" => 0,
      "security_review_count" => 0,
      "used_review_overrides" => [],
      "watcher" => nil
    }
  end

  defp persist_progress_entry(state, issue_id, entry, receipt) do
    progress = Map.put(state.progress, issue_id, entry)

    case HoldStore.persist_progress(Config.local_workspace_root(), progress) do
      :ok ->
        {:ok, %{state | progress: progress, progress_state_available: true}, receipt}

      {:error, reason} ->
        Logger.error("Failed to persist progress state issue_id=#{issue_id} reason=#{inspect(reason)}; refusing progress/review transition")

        unavailable_state =
          state
          |> Map.put(:progress_state_available, false)
          |> hold_progress_persistence_failure(issue_id)

        {:error, :progress_state_unavailable, unavailable_state}
    end
  end

  defp hold_progress_persistence_failure(state, issue_id) do
    case Map.get(state.running, issue_id) do
      nil ->
        state

      running_entry ->
        hold_input_token_budget_issue(
          state,
          issue_id,
          running_entry,
          "progress_state_unavailable",
          Map.get(running_entry, :codex_input_tokens, 0)
        )
    end
  end

  defp continue_after_turn?(server, issue_id) do
    GenServer.call(server, {:continue_after_turn, issue_id}, 5_000)
  end

  defp waiting_watcher?(state, issue_id) do
    get_in(state.progress, [issue_id, "watcher", "state"]) == "waiting"
  end

  defp waiting_watcher_issue_ids(progress) do
    Enum.flat_map(progress, fn {issue_id, entry} ->
      if get_in(entry, ["watcher", "state"]) == "waiting", do: [issue_id], else: []
    end)
  end

  defp restore_deferred_watchers(progress) do
    Enum.reduce(progress, %{}, fn {issue_id, entry}, deferred ->
      case Map.get(entry, "watcher") do
        %{"state" => "waiting"} = watcher ->
          metadata = %{
            identifier: Map.get(watcher, "identifier") || Map.get(entry, "identifier"),
            worker_host: Map.get(watcher, "worker_host"),
            workspace_path: Map.get(watcher, "workspace_path") || Map.get(entry, "workspace_path"),
            phase_budget: nil,
            started_at: DateTime.utc_now()
          }

          Map.put(deferred, issue_id, metadata)

        _watcher ->
          deferred
      end
    end)
  end

  defp disable_restored_waiters(state) do
    Enum.reduce(waiting_watcher_issue_ids(state.progress), state, fn issue_id, restored_state ->
      watcher_token = get_in(restored_state.progress, [issue_id, "watcher", "token"])

      Logger.warning("Disabled restored deferred waiter issue_id=#{issue_id}; scheduling a fail-closed continuation")

      settle_missing_waiter_receipt(
        restored_state,
        issue_id,
        watcher_token,
        :deferred_wait_disabled
      )
    end)
  end

  defp deferred_metadata(running_entry) do
    %{
      identifier: running_entry.identifier,
      issue_url: Map.get(running_entry.issue, :url),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      phase_budget: deferred_phase_budget(running_entry),
      started_at: Map.get(running_entry, :started_at)
    }
  end

  defp deferred_phase_budget(%{resume_phase: phase} = running_entry) when is_binary(phase) do
    %{
      phase: phase,
      requested_additional_input_tokens: Map.get(running_entry, :requested_additional_input_tokens),
      effective_additional_input_tokens: Map.get(running_entry, :effective_additional_input_tokens)
    }
  end

  defp deferred_phase_budget(_running_entry), do: nil

  defp remove_deferred_waiter_task(state, issue_id, watcher_token) do
    case Map.get(state.waiter_tasks, issue_id) do
      %{token: ^watcher_token} -> %{state | waiter_tasks: Map.delete(state.waiter_tasks, issue_id)}
      _task -> state
    end
  end

  defp stop_deferred_waiter_task(state, issue_id) do
    case Map.pop(state.waiter_tasks, issue_id) do
      {nil, _waiter_tasks} ->
        state

      {task, waiter_tasks} ->
        stop_waiter_task(state.task_supervisor, waiter_task_pid(task))
        %{state | waiter_tasks: waiter_tasks}
    end
  end

  defp stop_waiter_task(task_supervisor, pid) when is_pid(pid) do
    try do
      Task.Supervisor.terminate_child(task_supervisor, pid)
    catch
      :exit, _reason -> if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    end

    :ok
  end

  defp stop_waiter_task(_task_supervisor, _pid), do: :ok

  defp waiter_task_pid(pid) when is_pid(pid), do: pid
  defp waiter_task_pid(%{pid: pid}) when is_pid(pid), do: pid
  defp waiter_task_pid(_task), do: nil

  defp settle_missing_waiter_receipt(state, issue_id, watcher_token, result) do
    watcher = get_in(state.progress, [issue_id, "watcher"])

    if is_map(watcher) and Map.get(watcher, "state") == "waiting" and
         Map.get(watcher, "token") == watcher_token do
      settle_deferred_watcher(state, issue_id, watcher, %{
        "type" => "terminal",
        "status" => "receipt_invalid",
        "reason" => inspect(result),
        "expectedHead" => Map.get(watcher, "expected_head"),
        "observedHead" => Map.get(watcher, "expected_head")
      })
    else
      state
    end
  end

  defp poll_deferred_watcher(state, issue_id, watcher_token) do
    watcher = get_in(state.progress, [issue_id, "watcher"])

    cond do
      not active_watcher?(watcher, watcher_token) ->
        state

      watcher_expired?(watcher) ->
        settle_deferred_watcher(state, issue_id, watcher, %{
          "type" => "terminal",
          "status" => "timed_out",
          "expectedHead" => Map.get(watcher, "expected_head"),
          "observedHead" => Map.get(watcher, "expected_head")
        })

      true ->
        poll_active_watcher(state, issue_id, watcher, watcher_token)
    end
  end

  defp active_watcher?(watcher, watcher_token) do
    is_map(watcher) and Map.get(watcher, "state") == "waiting" and
      Map.get(watcher, "token") == watcher_token
  end

  defp watcher_expired?(watcher) do
    System.system_time(:millisecond) >= Map.get(watcher, "deadline_unix_ms", 0)
  end

  defp poll_active_watcher(state, issue_id, watcher, watcher_token) do
    case read_last_receipt(Map.fetch!(watcher, "receipt_path")) do
      {:ok, receipt, signature} ->
        updated_state =
          if signature == Map.get(watcher, "last_receipt_hash") do
            state
          else
            record_watcher_receipt_transition(state, issue_id, watcher, receipt, signature)
          end

        if waiting_watcher?(updated_state, issue_id) do
          schedule_deferred_watcher_poll(issue_id, watcher_token)
        end

        updated_state

      :missing ->
        schedule_deferred_watcher_poll(issue_id, watcher_token)
        state

      {:error, reason} ->
        settle_deferred_watcher(state, issue_id, watcher, %{
          "type" => "terminal",
          "status" => "receipt_invalid",
          "reason" => inspect(reason),
          "expectedHead" => Map.get(watcher, "expected_head"),
          "observedHead" => nil
        })
    end
  end

  defp schedule_deferred_watcher_poll(issue_id, watcher_token) do
    Process.send_after(
      self(),
      {:poll_deferred_watcher, issue_id, watcher_token},
      @watcher_poll_interval_ms
    )
  end

  defp read_last_receipt(path) when is_binary(path) do
    case File.read(path) do
      {:ok, encoded} ->
        encoded
        |> String.split("\n", trim: true)
        |> List.last()
        |> decode_receipt_line()

      {:error, :enoent} ->
        :missing

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_receipt_line(nil), do: :missing

  defp decode_receipt_line(line) do
    case Jason.decode(line) do
      {:ok, %{} = receipt} -> {:ok, receipt, canonical_hash(receipt)}
      {:ok, _other} -> {:error, :invalid_receipt}
      {:error, reason} -> {:error, reason}
    end
  end

  defp record_watcher_receipt_transition(state, issue_id, watcher, receipt, signature) do
    if Map.get(receipt, "type") == "terminal" do
      settle_deferred_watcher(state, issue_id, watcher, receipt)
    else
      progress = Map.fetch!(state.progress, issue_id)
      updated_watcher = Map.put(watcher, "last_receipt_hash", signature)

      fingerprint =
        progress
        |> Map.get("fingerprint", %{})
        |> Map.put("hosted_receipt", %{"state" => "provider_transition"})

      updated =
        progress
        |> Map.put("watcher", updated_watcher)
        |> Map.put("fingerprint", fingerprint)
        |> Map.put("progress_fingerprint_hash", canonical_hash(fingerprint))
        |> Map.put("last_progress_kind", "provider_transition")
        |> Map.put("last_progress_receipt", signature)
        |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())

      persist_progress_map(state, Map.put(state.progress, issue_id, updated), issue_id)
    end
  end

  defp settle_deferred_watcher(state, issue_id, watcher, receipt) do
    expected_head = Map.get(watcher, "expected_head")
    status = Map.get(receipt, "status", "receipt_invalid")
    receipt_expected_head = Map.get(receipt, "expectedHead")
    observed_head = Map.get(receipt, "observedHead")

    valid_terminal? =
      status in @terminal_watcher_states and receipt_expected_head == expected_head and
        (status == "head_changed" or observed_head == expected_head)

    terminal_status = if valid_terminal?, do: status, else: "receipt_invalid"
    progress = Map.fetch!(state.progress, issue_id)

    updated_watcher =
      watcher
      |> Map.put("state", terminal_status)
      |> Map.put("wake_count", 1)
      |> Map.put("terminal_receipt", receipt)
      |> Map.put("completed_at", DateTime.utc_now() |> DateTime.to_iso8601())

    fingerprint =
      progress
      |> Map.get("fingerprint", %{})
      |> Map.put("hosted_receipt", %{
        "state" => terminal_status,
        "expected_head" => expected_head,
        "observed_head" => observed_head,
        "receipt_path" => Map.get(watcher, "receipt_path")
      })

    updated =
      progress
      |> Map.put("watcher", updated_watcher)
      |> Map.put("fingerprint", fingerprint)
      |> Map.put("progress_fingerprint_hash", canonical_hash(fingerprint))
      |> Map.put("last_progress_kind", "provider_transition")
      |> Map.put("last_progress_receipt", canonical_hash(receipt))
      |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())

    persisted_state = persist_progress_map(state, Map.put(state.progress, issue_id, updated), issue_id)

    if persisted_state.progress_state_available do
      stopped_state = stop_deferred_waiter_task(persisted_state, issue_id)
      {metadata, deferred} = Map.pop(stopped_state.deferred, issue_id)
      wake_deferred_issue_once(%{stopped_state | deferred: deferred}, issue_id, terminal_status, metadata)
    else
      persisted_state
    end
  end

  defp persist_progress_map(state, progress, issue_id) do
    case HoldStore.persist_progress(Config.local_workspace_root(), progress) do
      :ok ->
        %{state | progress: progress, progress_state_available: true}

      {:error, reason} ->
        Logger.error("Failed to persist progress state issue_id=#{issue_id} reason=#{inspect(reason)}; refusing deferred transition")

        state
        |> Map.put(:progress_state_available, false)
        |> hold_progress_persistence_failure(issue_id)
    end
  end

  defp wake_deferred_issue_once(state, issue_id, terminal_status, metadata) do
    cond do
      Map.has_key?(state.running, issue_id) ->
        state

      is_map(metadata) ->
        phase_budget =
          Map.get(metadata, :phase_budget) || hold_phase_budget(Map.get(state.holds, issue_id))

        schedule_issue_retry(state, issue_id, 1, %{
          identifier: metadata.identifier,
          issue_url: Map.get(metadata, :issue_url),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          phase_budget: phase_budget,
          delay_type: :continuation,
          error: "deferred wait terminal: #{terminal_status}"
        })

      true ->
        state
    end
  end

  defp hold_phase_budget(%{resume_phase: phase} = hold) when is_binary(phase) do
    %{
      phase: phase,
      requested_additional_input_tokens: Map.get(hold, :requested_additional_input_tokens),
      effective_additional_input_tokens: Map.get(hold, :effective_additional_input_tokens)
    }
  end

  defp hold_phase_budget(_hold), do: nil

  defp valid_commit_sha?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}\z/i, value)

  defp normalize_commit_sha(value) do
    if valid_commit_sha?(value),
      do: {:ok, String.downcase(value)},
      else: {:error, :invalid_commit_sha}
  end

  defp valid_sha_container?(value) when is_binary(value), do: valid_commit_sha?(value)

  defp valid_sha_container?(value) when is_map(value) and map_size(value) > 0,
    do: Enum.all?(value, fn {_key, sha} -> valid_commit_sha?(sha) end)

  defp valid_sha_container?(_value), do: false

  defp valid_checksum_container?(value) when is_binary(value), do: String.trim(value) != ""

  defp valid_checksum_container?(value) when is_map(value) and map_size(value) > 0,
    do: Enum.all?(value, fn {_key, checksum} -> is_binary(checksum) and String.trim(checksum) != "" end)

  defp valid_checksum_container?(_value), do: false

  defp normalize_checksum_container(value) when is_binary(value) do
    case Regex.run(@sha256_checksum_pattern, value, capture: :all_but_first) do
      [hex] -> String.downcase(hex)
      _no_sha256_checksum -> value
    end
  end

  defp normalize_checksum_container(value) when is_map(value) do
    Map.new(value, fn {key, checksum} -> {key, normalize_checksum_container(checksum)} end)
  end

  defp normalize_checksum_container(value), do: value

  defp fingerprint_contains_head?(fingerprint, head) do
    case Map.get(fingerprint, "head_sha") do
      stored when is_binary(stored) -> String.downcase(stored) == head
      heads when is_map(heads) -> Enum.any?(Map.values(heads), &(is_binary(&1) and String.downcase(&1) == head))
      _ -> false
    end
  end

  defp canonical_hash(value) do
    value
    |> canonical_term()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp canonical_term(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), canonical_term(nested)} end)
    |> Enum.sort()
  end

  defp canonical_term(value) when is_list(value), do: Enum.map(value, &canonical_term/1)
  defp canonical_term(value), do: value

  defp progress_metadata(state, issue_id) do
    progress = Map.get(state.progress, issue_id, %{})

    %{
      progress_fingerprint: Map.get(progress, "fingerprint"),
      progress_fingerprint_hash: Map.get(progress, "progress_fingerprint_hash"),
      review_fingerprint_hash: Map.get(progress, "review_fingerprint_hash")
    }
  end

  defp blocked_issue_state(%{issue: %Issue{state: state}}), do: state
  defp blocked_issue_state(_metadata), do: nil

  defp blocked_issue_url(%{issue: %Issue{url: url}}), do: url
  defp blocked_issue_url(_metadata), do: nil

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    codex_app_server_port = Map.get(running_entry, :codex_app_server_port)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_app_server_port: codex_app_server_port_for_update(codex_app_server_port, update),
        thread_id: Map.get(update, :thread_id, Map.get(running_entry, :thread_id)),
        turn_id: Map.get(update, :turn_id, Map.get(running_entry, :turn_id)),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      }),
      token_delta
    }
  end

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp codex_app_server_port_for_update(_existing, %{codex_app_server_port: port})
       when is_port(port),
       do: port

  defp codex_app_server_port_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    %{
      event: update[:event],
      message: update[:payload] || update[:raw],
      timestamp: update[:timestamp]
    }
  end

  defp schedule_tick(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.tick_timer_ref) do
      Process.cancel_timer(state.tick_timer_ref)
    end

    tick_token = make_ref()
    timer_ref = Process.send_after(self(), {:tick, tick_token}, delay_ms)

    %{
      state
      | tick_timer_ref: timer_ref,
        tick_token: tick_token,
        next_poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    {Map.get(state.running, issue_id), %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp refresh_runtime_config(%State{} = state) do
    config = Config.settings!()

    %{
      state
      | poll_interval_ms: config.polling.interval_ms,
        max_concurrent_agents: config.agent.max_concurrent_agents
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_codex_rate_limits(state, _update), do: state

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    running_entry = running_entry || %{}
    usage = extract_token_usage(update)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    cond do
      rate_limits_map?(direct) ->
        direct

      rate_limits_map?(payload) ->
        payload

      true ->
        rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limits_map?(payload) when is_map(payload) do
    limit_id =
      Map.get(payload, "limit_id") ||
        Map.get(payload, :limit_id) ||
        Map.get(payload, "limit_name") ||
        Map.get(payload, :limit_name)

    has_buckets =
      Enum.any?(
        ["primary", :primary, "secondary", :secondary, "credits", :credits],
        &Map.has_key?(payload, &1)
      )

    !is_nil(limit_id) and has_buckets
  end

  defp rate_limits_map?(_payload), do: false

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
