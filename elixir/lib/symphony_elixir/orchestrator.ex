defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls Linear and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{AgentRunner, Config, HoldStore, ProcessTree, RunnerCapabilities}
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.{SSH, StatusDashboard, Tracker, Workspace}

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  @token_warning_ack_timeout_ms 5_000
  @hold_state_persist_retry_delay_ms 1_000
  @default_process_cleanup_timeout_ms 2_000
  @resume_phases ~w(implementation validation review-fix hosted-closeout landing)
  @phase_resume_pending_reason "input_token_resume_pending"
  @budget_hold_reasons ~w(input_token_limit input_token_warning_unsupported input_token_checkpoint input_token_checkpoint_grace input_token_checkpoint_failed input_token_resume_pending)
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
      hold_store_available: true,
      hold_state_persist_retry_timer_ref: nil,
      hold_state_persist_retry_token: nil,
      running: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      retry_attempts: %{},
      holds: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    capability_preflight = Keyword.get(opts, :runner_capability_preflight, &RunnerCapabilities.preflight/0)

    case capability_preflight.() do
      :ok -> initialize_state()
      {:error, reason} -> {:stop, {:runner_capability_preflight_failed, reason}}
    end
  end

  defp initialize_state do
    now_ms = System.monotonic_time(:millisecond)
    config = Config.settings!()

    case HoldStore.load(config.workspace.root) do
      {:ok, holds} ->
        state = %State{
          poll_interval_ms: config.polling.interval_ms,
          max_concurrent_agents: config.agent.max_concurrent_agents,
          next_poll_due_at_ms: now_ms,
          poll_check_in_progress: false,
          tick_timer_ref: nil,
          tick_token: nil,
          holds: holds,
          claimed: holds |> Map.keys() |> MapSet.new(),
          codex_totals: @empty_codex_totals,
          codex_rate_limits: nil
        }

        if map_size(holds) > 0 do
          Logger.info("Restored durable issue holds count=#{map_size(holds)}")
        end

        state = maybe_pause_for_pending_cleanup(state, holds)

        run_terminal_workspace_cleanup(holds)
        {:ok, schedule_tick(state, 0)}

      {:error, reason} ->
        Logger.error("Failed to restore durable issue holds: #{inspect(reason)}")
        {:stop, {:hold_state_load_failed, reason}}
    end
  end

  defp maybe_pause_for_pending_cleanup(state, holds) do
    if Enum.any?(holds, fn {_issue_id, hold} -> Map.get(hold, :cleanup_pending, false) end) do
      Logger.warning("Restored pending issue cleanup; keeping dispatch paused until cleanup is confirmed")
      schedule_hold_state_persist_retry(state)
    else
      state
    end
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

        state = handle_running_task_exit(state, issue_id, running_entry, session_id, reason)

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
          |> maybe_put_runtime_value(:codex_app_server_pid, runtime_info[:codex_app_server_pid])
          |> maybe_put_runtime_value(:codex_app_server_port, runtime_info[:codex_app_server_port])
          |> maybe_refresh_progress_for_runtime_info(runtime_info)

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

        updated_running_entry =
          track_input_token_warning_reader_activity(updated_running_entry, update.event)

        state =
          state
          |> apply_codex_token_delta(token_delta)
          |> apply_codex_rate_limits(update)

        state = %{state | running: Map.put(running, issue_id, updated_running_entry)}

        state =
          cond do
            token_count_update?(update) ->
              enforce_input_token_budget(state, issue_id)

            update.event == :token_budget_warning_unsupported ->
              enforce_input_token_warning_delivery(state, issue_id)

            true ->
              state
          end

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:input_token_warning_ack_timeout, issue_id, timeout_token}, state) do
    state =
      case Map.get(state.running, issue_id) do
        %{
          input_token_warning_status: "requested",
          input_token_warning_ack_token: ^timeout_token
        } = running_entry ->
          if Map.get(running_entry, :input_token_warning_reader_busy, false) do
            updated_entry = reset_input_token_warning_ack_timeout(running_entry)
            %{state | running: Map.put(state.running, issue_id, updated_entry)}
          else
            updated_entry =
              running_entry
              |> Map.put(:input_token_warning_status, "unsupported")
              |> clear_input_token_warning_ack_timeout()

            state
            |> Map.put(:running, Map.put(state.running, issue_id, updated_entry))
            |> enforce_input_token_warning_delivery(issue_id)
          end

        _ ->
          state
      end

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:hold_state_persist_retry, retry_token}, state) do
    state =
      case state do
        %{hold_state_persist_retry_token: ^retry_token} ->
          candidate_state = clear_hold_state_persist_retry(state)
          {candidate_state, cleanup_complete?} = retry_quarantined_running_cleanup(candidate_state)

          case persist_hold_state(candidate_state) do
            :ok when cleanup_complete? ->
              Logger.info("Durable hold state recovered; issue dispatch may resume")
              %{candidate_state | hold_store_available: true}

            :ok ->
              Logger.error("Durable hold state recovered but quarantined process cleanup is still pending; keeping issue dispatch paused")
              schedule_hold_state_persist_retry(candidate_state)

            {:error, reason} ->
              Logger.error("Durable hold state is still unavailable; keeping issue dispatch paused reason=#{inspect(reason)}")
              schedule_hold_state_persist_retry(candidate_state)
          end

        _ ->
          state
      end

    notify_dashboard()
    {:noreply, state}
  end

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

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp handle_running_task_exit(
         state,
         issue_id,
         %{input_token_warning_status: "requested"} = running_entry,
         _session_id,
         _reason
       ) do
    hold_exited_issue_with_unacknowledged_warning(state, issue_id, running_entry)
  end

  defp handle_running_task_exit(
         state,
         issue_id,
         %{input_token_warning_status: "delivered"} = running_entry,
         _session_id,
         :normal
       ) do
    hold_checkpointed_issue(state, issue_id, running_entry, "input_token_checkpoint")
  end

  defp handle_running_task_exit(
         state,
         issue_id,
         %{input_token_warning_status: "delivered"} = running_entry,
         _session_id,
         _reason
       ) do
    hold_checkpointed_issue(state, issue_id, running_entry, "input_token_checkpoint_failed")
  end

  defp handle_running_task_exit(
         state,
         issue_id,
         %{resume_phase: phase} = running_entry,
         _session_id,
         :normal
       )
       when is_binary(phase) do
    hold_checkpointed_issue(state, issue_id, running_entry, "input_token_checkpoint")
  end

  defp handle_running_task_exit(
         state,
         issue_id,
         %{resume_phase: phase} = running_entry,
         _session_id,
         _reason
       )
       when is_binary(phase) do
    hold_checkpointed_issue(state, issue_id, running_entry, "input_token_checkpoint_failed")
  end

  defp handle_running_task_exit(state, issue_id, running_entry, session_id, :normal) do
    Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

    state
    |> complete_issue(issue_id)
    |> schedule_issue_retry(issue_id, 1, %{
      identifier: running_entry.identifier,
      delay_type: :continuation,
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      worker_affinity: Map.get(running_entry, :worker_affinity)
    })
  end

  defp handle_running_task_exit(state, issue_id, running_entry, session_id, reason) do
    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

    schedule_issue_retry(state, issue_id, next_retry_attempt_from_running(running_entry), %{
      identifier: running_entry.identifier,
      error: "agent exited: #{inspect(reason)}",
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      worker_affinity: Map.get(running_entry, :worker_affinity)
    })
  end

  @impl true
  def terminate(_reason, %State{running: running}) do
    terminate_codex_process_trees(Map.values(running))
    :ok
  end

  defp maybe_dispatch(%State{} = state) do
    state = state |> reconcile_running_issues() |> reconcile_held_issues()

    if state.hold_store_available do
      dispatch_available_issues(state)
    else
      Logger.warning("Skipping issue dispatch while durable hold state is unavailable")
      state
    end
  end

  defp dispatch_available_issues(%State{} = state) do
    with :ok <- Config.validate!(),
         {:ok, issues} <- Tracker.fetch_candidate_issues(),
         true <- available_slots(state) > 0 do
      choose_issues(issues, state)
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Linear API token missing in WORKFLOW.md")
        state

      {:error, :missing_linear_project_slug} ->
        Logger.error("Linear project slug missing in WORKFLOW.md")
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
        Logger.error("Failed to fetch from Linear: #{inspect(reason)}")
        state

      false ->
        state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issue_states_by_ids(running_ids) do
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

  defp reconcile_held_issues(%State{holds: holds} = state) when map_size(holds) == 0, do: state

  defp reconcile_held_issues(%State{holds: holds} = state) do
    issue_ids = Map.keys(holds)

    case Tracker.fetch_issue_states_by_ids(issue_ids) do
      {:ok, issues} ->
        visible = Map.new(issues, &{&1.id, &1})

        Enum.reduce(issue_ids, state, fn issue_id, state_acc ->
          reconcile_held_issue(state_acc, issue_id, Map.get(visible, issue_id))
        end)

      {:error, reason} ->
        Logger.debug("Failed to refresh held issue states: #{inspect(reason)}; keeping holds")
        state
    end
  end

  defp reconcile_held_issue(state, issue_id, %Issue{} = issue) do
    hold = Map.fetch!(state.holds, issue_id)

    if hold_cleanup_pending?(state, issue_id, hold) do
      Logger.debug("Held issue cleanup is still pending; keeping hold issue_id=#{issue_id}")
      state
    else
      reconcile_clean_hold(state, issue_id, hold, issue)
    end
  end

  defp reconcile_held_issue(state, issue_id, nil) do
    Logger.debug("Held issue is not currently visible; keeping hold until a tracker state is verified issue_id=#{issue_id}")
    state
  end

  defp hold_cleanup_pending?(state, issue_id, hold) do
    Map.get(hold, :cleanup_pending, false) or Map.has_key?(state.running, issue_id)
  end

  defp reconcile_clean_hold(state, issue_id, hold, issue) do
    if budget_hold?(hold) do
      reconcile_budget_hold(state, issue_id, hold, issue)
    else
      reconcile_manual_hold(state, issue_id, hold, issue)
    end
  end

  defp reconcile_budget_hold(state, issue_id, hold, issue) do
    if is_binary(issue.state) and
         normalize_issue_state(issue.state) != normalize_issue_state(hold.issue_state) do
      Logger.info("Token-budget hold survives tracker state change: issue_id=#{issue_id} old_state=#{inspect(hold.issue_state)} new_state=#{inspect(issue.state)}")
      persist_updated_hold(state, issue_id, %{hold | issue_state: issue.state})
    else
      state
    end
  end

  defp reconcile_manual_hold(state, issue_id, hold, issue) do
    cond do
      not is_binary(issue.state) ->
        Logger.debug("Held issue returned without a verifiable tracker state; keeping hold issue_id=#{issue_id}")
        state

      not is_binary(hold.issue_state) ->
        updated_hold = %{hold | issue_state: issue.state}
        persist_updated_hold(state, issue_id, updated_hold)

      normalize_issue_state(issue.state) == normalize_issue_state(hold.issue_state) ->
        state

      true ->
        Logger.info("Issue state changed while held: #{issue_context(issue)} old_state=#{inspect(hold.issue_state)} new_state=#{inspect(issue.state)}; releasing hold")

        if terminal_issue_state?(issue.state, terminal_state_set()) do
          cleanup_issue_workspace(issue.identifier, Map.get(hold, :worker_host))
        end

        release_issue_hold(state, issue_id)
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

  @doc false
  @spec claim_issue_for_dispatch_for_test(Issue.t(), (String.t(), String.t() -> :ok | {:error, term()})) ::
          Issue.t()
  def claim_issue_for_dispatch_for_test(%Issue{} = issue, issue_state_updater)
      when is_function(issue_state_updater, 2) do
    claim_issue_for_dispatch(issue, issue_state_updater)
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

      !issue_routable_to_worker?(issue) ->
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

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace) do
    case terminate_running_issue_with_result(state, issue_id, cleanup_workspace) do
      {:ok, updated_state} ->
        updated_state

      {:error, reason, unchanged_state} ->
        Logger.error("Failed to terminate active issue process tree issue_id=#{issue_id} reason=#{inspect(reason)}")
        unchanged_state
    end
  end

  defp terminate_running_issue_with_result(%State{} = state, issue_id, cleanup_workspace) do
    case Map.get(state.running, issue_id) do
      nil ->
        {:ok, release_issue_claim(state, issue_id)}

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        case terminate_codex_process_tree(running_entry) do
          :ok ->
            {:ok,
             finalize_running_issue_termination(
               state,
               issue_id,
               running_entry,
               identifier,
               pid,
               ref,
               cleanup_workspace
             )}

          {:error, reason} ->
            {:error, reason, state}
        end

      _ ->
        {:ok, release_issue_claim(state, issue_id)}
    end
  end

  defp finalize_running_issue_termination(
         state,
         issue_id,
         running_entry,
         identifier,
         pid,
         ref,
         cleanup_workspace
       ) do
    state = record_session_completion_totals(state, running_entry)
    worker_host = Map.get(running_entry, :worker_host)

    if cleanup_workspace, do: cleanup_issue_workspace(identifier, worker_host)
    if is_pid(pid), do: terminate_task(pid)
    if is_reference(ref), do: Process.demonitor(ref, [:flush])

    %{
      state
      | running: Map.delete(state.running, issue_id),
        claimed: MapSet.delete(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp enforce_input_token_budget(%State{} = state, issue_id) do
    case Map.get(state.running, issue_id) do
      %{input_token_limit: limit, codex_input_tokens: observed} = running_entry
      when is_integer(limit) and limit > 0 and is_integer(observed) and observed >= limit ->
        enforce_input_token_hard_limit(state, issue_id, running_entry, limit, observed)

      %{codex_input_tokens: observed} = running_entry
      when is_integer(observed) ->
        if checkpoint_grace_exhausted?(running_entry, observed) do
          enforce_input_token_checkpoint_grace(state, issue_id, running_entry, observed)
        else
          maybe_enforce_input_token_warning(state, issue_id, running_entry, observed)
        end

      _ ->
        state
    end
  end

  defp maybe_enforce_input_token_warning(state, issue_id, running_entry, observed) do
    case Map.get(running_entry, :input_token_limit) do
      limit when is_integer(limit) and limit > 0 ->
        maybe_warn_input_token_budget(state, issue_id, running_entry, limit, observed)

      _ ->
        state
    end
  end

  defp checkpoint_grace_exhausted?(running_entry, observed) do
    baseline = Map.get(running_entry, :input_token_warning_observed_at)
    grace = Map.get(running_entry, :input_token_checkpoint_grace)

    is_integer(baseline) and is_integer(grace) and grace > 0 and observed - baseline >= grace
  end

  defp enforce_input_token_checkpoint_grace(state, issue_id, running_entry, observed) do
    limit = Map.get(running_entry, :input_token_limit)

    case hold_token_budget_issue(
           state,
           issue_id,
           running_entry,
           "input_token_checkpoint_grace",
           limit,
           observed
         ) do
      {:ok, updated_state, _hold} -> updated_state
      {:error, updated_state, _reason} -> updated_state
    end
  end

  defp hold_checkpointed_issue(state, issue_id, running_entry, reason) do
    limit = Map.get(running_entry, :input_token_limit)
    observed = Map.get(running_entry, :codex_input_tokens, 0)

    case hold_token_budget_issue(state, issue_id, running_entry, reason, limit, observed) do
      {:ok, updated_state, _hold} -> updated_state
      {:error, updated_state, _reason} -> updated_state
    end
  end

  defp enforce_input_token_hard_limit(state, issue_id, running_entry, limit, observed) do
    case hold_token_budget_issue(state, issue_id, running_entry, "input_token_limit", limit, observed) do
      {:ok, updated_state, _hold} -> updated_state
      {:error, updated_state, _reason} -> updated_state
    end
  end

  defp token_count_update?(%{payload: payload}) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    method in [
      "codex/event/token_count",
      "thread/tokenUsage/updated",
      :token_count,
      :thread_token_usage_updated
    ]
  end

  defp token_count_update?(%{event: :token_count}), do: true
  defp token_count_update?(_update), do: false

  defp maybe_warn_input_token_budget(state, issue_id, running_entry, limit, observed) do
    warning_ratio = Map.get(running_entry, :input_token_warning_ratio, 0.70)
    warning_threshold = ceil(limit * warning_ratio)

    if observed >= warning_threshold and Map.get(running_entry, :input_token_warning_sent, false) == false do
      warning_status = steer_token_budget_warning(running_entry)

      updated_entry =
        build_input_token_warning_entry(
          running_entry,
          warning_status,
          warning_threshold,
          observed
        )

      Logger.warning(
        "Input-token warning threshold reached: issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} observed_tokens=#{observed} limit=#{limit} warning_ratio=#{warning_ratio} steering_status=#{warning_status}"
      )

      state = %{state | running: Map.put(state.running, issue_id, updated_entry)}

      if warning_status == "unsupported" do
        enforce_input_token_warning_delivery(state, issue_id)
      else
        state
      end
    else
      state
    end
  end

  defp enforce_input_token_warning_delivery(state, issue_id) do
    case Map.get(state.running, issue_id) do
      %{input_token_warning_status: "unsupported"} = running_entry ->
        limit = Map.get(running_entry, :input_token_limit)
        observed = Map.get(running_entry, :codex_input_tokens, 0)

        case hold_token_budget_issue(
               state,
               issue_id,
               running_entry,
               "input_token_warning_unsupported",
               limit,
               observed
             ) do
          {:ok, updated_state, _hold} -> updated_state
          {:error, updated_state, _reason} -> updated_state
        end

      _ ->
        state
    end
  end

  defp steer_token_budget_warning(running_entry) do
    with port when is_port(port) <- Map.get(running_entry, :codex_app_server_port),
         thread_id when is_binary(thread_id) <- Map.get(running_entry, :thread_id),
         turn_id when is_binary(turn_id) <- Map.get(running_entry, :turn_id),
         :ok <- AppServer.steer_turn(port, thread_id, turn_id, token_budget_warning_instruction()) do
      "requested"
    else
      _ -> "unsupported"
    end
  end

  defp build_input_token_warning_entry(running_entry, "requested", warning_threshold, observed) do
    running_entry
    |> Map.put(:input_token_warning_sent, true)
    |> Map.put(:input_token_warning_status, "requested")
    |> Map.put(:input_token_warning_threshold, warning_threshold)
    |> Map.put(:input_token_warning_observed_at, observed)
    |> reset_input_token_warning_ack_timeout()
  end

  defp build_input_token_warning_entry(running_entry, warning_status, warning_threshold, observed) do
    running_entry
    |> Map.put(:input_token_warning_sent, true)
    |> Map.put(:input_token_warning_status, warning_status)
    |> Map.put(:input_token_warning_threshold, warning_threshold)
    |> Map.put(:input_token_warning_observed_at, observed)
  end

  defp reset_input_token_warning_ack_timeout(running_entry) do
    timeout_token = make_ref()

    timer_ref =
      Process.send_after(
        self(),
        {:input_token_warning_ack_timeout, running_entry.issue.id, timeout_token},
        @token_warning_ack_timeout_ms
      )

    running_entry
    |> clear_input_token_warning_ack_timeout()
    |> Map.put(:input_token_warning_ack_timer_ref, timer_ref)
    |> Map.put(:input_token_warning_ack_token, timeout_token)
  end

  defp settle_input_token_warning_response(running_entry, event)
       when event in [:token_budget_warning_delivered, :token_budget_warning_unsupported],
       do: clear_input_token_warning_ack_timeout(running_entry)

  defp settle_input_token_warning_response(running_entry, _event), do: running_entry

  defp track_input_token_warning_reader_activity(running_entry, :tool_call_started) do
    Map.put(running_entry, :input_token_warning_reader_busy, true)
  end

  defp track_input_token_warning_reader_activity(running_entry, event)
       when event in [:tool_call_completed, :tool_call_failed, :unsupported_tool_call] do
    running_entry = Map.put(running_entry, :input_token_warning_reader_busy, false)

    if Map.get(running_entry, :input_token_warning_status) == "requested" do
      reset_input_token_warning_ack_timeout(running_entry)
    else
      running_entry
    end
  end

  defp track_input_token_warning_reader_activity(running_entry, _event), do: running_entry

  defp clear_input_token_warning_ack_timeout(running_entry) do
    running_entry
    |> cancel_timer(:input_token_warning_ack_timer_ref)
    |> Map.put(:input_token_warning_ack_timer_ref, nil)
    |> Map.put(:input_token_warning_ack_token, nil)
  end

  defp cancel_timer(running_entry, key) do
    case Map.get(running_entry, key) do
      timer_ref when is_reference(timer_ref) ->
        Process.cancel_timer(timer_ref)
        running_entry

      _ ->
        running_entry
    end
  end

  defp token_budget_warning_instruction do
    "Checkpoint only. Finish the already-running atomic tool call, update the persistent workpad with completed work, remaining tasks, validation status, exact HEAD, and stop conditions, then end this turn. Do not start new implementation, review, validation, or waiting work."
  end

  defp hold_token_budget_issue(state, issue_id, running_entry, reason, limit, observed) do
    hold_running_issue(state, issue_id, running_entry, reason, limit, observed, true)
  end

  defp hold_running_issue(state, issue_id, running_entry, reason, limit, observed) do
    hold_running_issue(state, issue_id, running_entry, reason, limit, observed, false)
  end

  defp hold_running_issue(state, issue_id, running_entry, reason, limit, observed, stop_if_persist_fails) do
    hold = build_issue_hold(issue_id, running_entry, reason, limit, observed, true)

    held_state = %{
      state
      | holds: Map.put(state.holds, issue_id, hold),
        claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }

    case persist_hold_state(held_state) do
      :ok ->
        maybe_interrupt_turn(running_entry)

        held_state
        |> terminate_running_issue_with_result(issue_id, false)
        |> finish_running_hold_cleanup(held_state, issue_id, hold)

      {:error, persist_reason} ->
        handle_initial_hold_persist_failure(
          state,
          held_state,
          issue_id,
          running_entry,
          hold,
          persist_reason,
          stop_if_persist_fails
        )
    end
  end

  defp handle_initial_hold_persist_failure(
         _state,
         held_state,
         issue_id,
         running_entry,
         hold,
         persist_reason,
         true
       ) do
    Logger.error(
      "Durable token-budget hold persistence failed; stopping the run and pausing dispatch while persistence retries: issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} reason=#{inspect(persist_reason)}"
    )

    maybe_interrupt_turn(running_entry)

    quarantined_state =
      case terminate_running_issue_with_result(held_state, issue_id, false) do
        {:ok, terminated_state} ->
          cleaned_hold = Map.put(hold, :cleanup_pending, false)
          %{terminated_state | holds: Map.put(terminated_state.holds, issue_id, cleaned_hold)}

        {:error, cleanup_reason, unchanged_state} ->
          Logger.error("Token-budget process cleanup failed while hold persistence is unavailable: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{inspect(cleanup_reason)}")
          unchanged_state
      end

    {:error, schedule_hold_state_persist_retry(quarantined_state), :hold_state_unavailable}
  end

  defp handle_initial_hold_persist_failure(
         state,
         _held_state,
         issue_id,
         running_entry,
         _hold,
         persist_reason,
         false
       ) do
    Logger.error("Refusing to stop issue because durable hold persistence failed: issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} reason=#{inspect(persist_reason)}")

    {:error, state, :hold_state_unavailable}
  end

  defp schedule_hold_state_persist_retry(%State{} = state) do
    case state.hold_state_persist_retry_token do
      retry_token when is_reference(retry_token) ->
        %{state | hold_store_available: false}

      _ ->
        retry_token = make_ref()

        timer_ref =
          Process.send_after(
            self(),
            {:hold_state_persist_retry, retry_token},
            @hold_state_persist_retry_delay_ms
          )

        %{
          state
          | hold_store_available: false,
            hold_state_persist_retry_timer_ref: timer_ref,
            hold_state_persist_retry_token: retry_token
        }
    end
  end

  defp clear_hold_state_persist_retry(%State{} = state) do
    %{
      state
      | hold_state_persist_retry_timer_ref: nil,
        hold_state_persist_retry_token: nil
    }
  end

  defp retry_quarantined_running_cleanup(%State{} = state) do
    Enum.reduce(state.holds, {state, true}, &retry_quarantined_hold_cleanup/2)
  end

  defp retry_quarantined_hold_cleanup(
         {issue_id, %{cleanup_pending: true} = hold},
         {%State{} = state, all_complete?}
       ) do
    retry_quarantined_process_cleanup(state, issue_id, hold, all_complete?)
  end

  defp retry_quarantined_hold_cleanup(_hold_entry, state_and_completion),
    do: state_and_completion

  defp retry_quarantined_process_cleanup(state, issue_id, hold, all_complete?) do
    cleanup_result =
      if Map.has_key?(state.running, issue_id) do
        terminate_running_issue_with_result(state, issue_id, false)
      else
        case terminate_detached_hold_process_tree(hold) do
          :ok -> {:ok, state}
          {:error, reason} -> {:error, reason, state}
        end
      end

    case cleanup_result do
      {:ok, terminated_state} ->
        cleaned_hold = Map.put(hold, :cleanup_pending, false)
        {%{terminated_state | holds: Map.put(terminated_state.holds, issue_id, cleaned_hold)}, all_complete?}

      {:error, cleanup_reason, unchanged_state} ->
        Logger.error("Quarantined process cleanup is still failing: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{inspect(cleanup_reason)}")
        {unchanged_state, false}
    end
  end

  defp hold_exited_issue_with_unacknowledged_warning(state, issue_id, running_entry) do
    running_entry = clear_input_token_warning_ack_timeout(running_entry)
    limit = Map.get(running_entry, :input_token_limit)
    observed = Map.get(running_entry, :codex_input_tokens, 0)

    hold =
      build_issue_hold(
        issue_id,
        running_entry,
        "input_token_warning_unsupported",
        limit,
        observed,
        true
      )

    held_state = %{
      state
      | holds: Map.put(state.holds, issue_id, hold),
        claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }

    case persist_hold_state(held_state) do
      :ok ->
        finish_exited_warning_hold_cleanup(held_state, issue_id, hold)

      {:error, persist_reason} ->
        Logger.error(
          "Agent exited before token-budget warning acknowledgement and hold persistence failed; keeping in-memory quarantine and pausing dispatch: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{inspect(persist_reason)}"
        )

        schedule_hold_state_persist_retry(held_state)
    end
  end

  defp finish_exited_warning_hold_cleanup(state, issue_id, hold) do
    case terminate_detached_hold_process_tree(hold) do
      :ok ->
        case mark_hold_cleanup_complete(state, issue_id, hold) do
          {:ok, cleaned_state, _cleaned_hold} ->
            Logger.warning("Agent exited before token-budget warning acknowledgement; issue placed on durable hold after process cleanup: issue_id=#{issue_id} issue_identifier=#{hold.identifier}")
            cleaned_state

          {:error, safe_state, :hold_state_unavailable} ->
            Logger.error("Agent process cleanup completed but durable cleanup confirmation failed: issue_id=#{issue_id} issue_identifier=#{hold.identifier}; keeping dispatch paused")
            schedule_hold_state_persist_retry(safe_state)
        end

      {:error, cleanup_reason} ->
        Logger.error(
          "Agent exited before token-budget warning acknowledgement and detached process cleanup failed: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{inspect(cleanup_reason)}; keeping dispatch paused"
        )

        schedule_hold_state_persist_retry(state)
    end
  end

  defp terminate_detached_hold_process_tree(hold) do
    if detached_hold_cleanup_target?(hold) do
      terminate_codex_process_tree(hold)
    else
      {:error, :missing_process_cleanup_proof}
    end
  end

  defp detached_hold_cleanup_target?(%{worker_host: worker_host, workspace_path: workspace_path})
       when is_binary(worker_host) and is_binary(workspace_path),
       do: true

  defp detached_hold_cleanup_target?(hold), do: local_codex_os_pid(hold) != []

  defp build_issue_hold(issue_id, running_entry, reason, limit, observed, cleanup_pending) do
    %{
      issue_id: issue_id,
      identifier: running_entry.identifier,
      reason: reason,
      limit: limit,
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
      codex_app_server_pid: durable_local_codex_os_pid(running_entry),
      cleanup_pending: cleanup_pending,
      held_at: DateTime.utc_now()
    }
  end

  defp finish_running_hold_cleanup({:ok, terminated_state}, _held_state, issue_id, hold) do
    cleanup_pending_state = %{
      terminated_state
      | claimed: MapSet.put(terminated_state.claimed, issue_id),
        retry_attempts: Map.delete(terminated_state.retry_attempts, issue_id)
    }

    case mark_hold_cleanup_complete(cleanup_pending_state, issue_id, hold) do
      {:ok, cleaned_state, cleaned_hold} ->
        Logger.warning(
          "Issue placed on durable internal hold: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{hold.reason} observed_tokens=#{hold.observed_tokens} limit=#{inspect(hold.limit)}"
        )

        {:ok, cleaned_state, cleaned_hold}

      {:error, safe_state, :hold_state_unavailable} ->
        Logger.error("Issue cleanup completed but durable cleanup confirmation failed: issue_id=#{issue_id} issue_identifier=#{hold.identifier}; keeping dispatch paused")

        {:error, schedule_hold_state_persist_retry(safe_state), :hold_state_unavailable}
    end
  end

  defp finish_running_hold_cleanup(
         {:error, cleanup_reason, _unchanged_state},
         held_state,
         issue_id,
         hold
       ) do
    Logger.error("Issue is durably held but process cleanup failed: issue_id=#{issue_id} issue_identifier=#{hold.identifier} reason=#{inspect(cleanup_reason)}")

    {:error, schedule_hold_state_persist_retry(held_state), :cleanup_failed}
  end

  defp maybe_interrupt_turn(running_entry) do
    with port when is_port(port) <- Map.get(running_entry, :codex_app_server_port),
         thread_id when is_binary(thread_id) <- Map.get(running_entry, :thread_id),
         turn_id when is_binary(turn_id) <- Map.get(running_entry, :turn_id) do
      AppServer.interrupt_turn(port, thread_id, turn_id)
    else
      _ -> :ok
    end
  end

  defp release_issue_hold(%State{} = state, issue_id) do
    candidate_state = %{
      state
      | holds: Map.delete(state.holds, issue_id),
        claimed: MapSet.delete(state.claimed, issue_id)
    }

    case persist_hold_state(candidate_state) do
      :ok ->
        candidate_state

      {:error, reason} ->
        Logger.error("Failed to persist hold release issue_id=#{issue_id} reason=#{inspect(reason)}; keeping hold")
        state
    end
  end

  defp persist_updated_hold(%State{} = state, issue_id, updated_hold) do
    candidate_state = %{state | holds: Map.put(state.holds, issue_id, updated_hold)}

    case persist_hold_state(candidate_state) do
      :ok ->
        candidate_state

      {:error, reason} ->
        Logger.error("Failed to persist held issue tracker state issue_id=#{issue_id} reason=#{inspect(reason)}; keeping prior hold state")

        state
    end
  end

  defp persist_hold_state(%State{} = state) do
    HoldStore.persist(Config.settings!().workspace.root, state.holds)
  end

  defp mark_hold_cleanup_complete(%State{} = state, issue_id, hold) do
    cleaned_hold = Map.put(hold, :cleanup_pending, false)
    candidate_state = %{state | holds: Map.put(state.holds, issue_id, cleaned_hold)}

    case persist_hold_state(candidate_state) do
      :ok ->
        {:ok, candidate_state, cleaned_hold}

      {:error, reason} ->
        Logger.error("Failed to persist completed hold cleanup issue_id=#{issue_id} reason=#{inspect(reason)}; keeping dispatch paused")

        {:error, candidate_state, :hold_state_unavailable}
    end
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
          restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if is_integer(elapsed_ms) and elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

      next_attempt = next_retry_attempt_from_running(running_entry)

      case terminate_running_issue_with_result(state, issue_id, false) do
        {:ok, terminated_state} ->
          schedule_issue_retry(terminated_state, issue_id, next_attempt, %{
            identifier: identifier,
            error: "stalled for #{elapsed_ms}ms without codex activity"
          })

        {:error, reason, unchanged_state} ->
          Logger.error("Failed to clean up stalled issue process tree issue_id=#{issue_id} issue_identifier=#{identifier} reason=#{inspect(reason)}; keeping running state without retry")

          unchanged_state
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

  defp terminate_task(pid) when is_pid(pid) do
    case Task.Supervisor.terminate_child(SymphonyElixir.TaskSupervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid), do: :ok

  defp terminate_codex_process_tree(%{worker_host: worker_host, workspace_path: workspace_path})
       when is_binary(worker_host) and is_binary(workspace_path) do
    SSH.terminate_remote_process_tree(worker_host, workspace_path, process_cleanup_timeout_ms())
  end

  defp terminate_codex_process_tree(%{worker_host: worker_host}) when is_binary(worker_host) do
    {:error, {:remote_process_cleanup_unavailable, worker_host, :missing_workspace_path}}
  end

  defp terminate_codex_process_tree(running_entry) do
    terminate_codex_process_trees([running_entry])
  end

  defp terminate_codex_process_trees(running_entries) do
    remote_results =
      Enum.flat_map(running_entries, fn
        %{worker_host: worker_host, workspace_path: workspace_path}
        when is_binary(worker_host) and is_binary(workspace_path) ->
          result =
            SSH.terminate_remote_process_tree(
              worker_host,
              workspace_path,
              process_cleanup_timeout_ms()
            )

          if match?({:error, _reason}, result) do
            {:error, reason} = result
            Logger.error("Remote process cleanup failed worker_host=#{worker_host} reason=#{inspect(reason)}")
          end

          [result]

        _ ->
          []
      end)

    os_pids = Enum.flat_map(running_entries, &local_codex_os_pid/1)

    local_results =
      if os_pids == [] do
        []
      else
        [ProcessTree.terminate_os_process_trees(os_pids, process_cleanup_timeout_ms())]
      end

    cleanup_results(remote_results ++ local_results)
  end

  defp cleanup_results(results) do
    case Enum.filter(results, &match?({:error, _reason}, &1)) do
      [] -> :ok
      [{:error, reason}] -> {:error, reason}
      errors -> {:error, {:process_tree_cleanup_failed, Enum.map(errors, &elem(&1, 1))}}
    end
  end

  defp local_codex_os_pid(%{worker_host: worker_host}) when is_binary(worker_host), do: []

  defp local_codex_os_pid(running_entry) do
    case Map.get(running_entry, :codex_app_server_pid) do
      os_pid when is_integer(os_pid) and os_pid > 0 ->
        [os_pid]

      os_pid when is_binary(os_pid) ->
        case Integer.parse(os_pid) do
          {parsed_pid, ""} when parsed_pid > 0 -> [parsed_pid]
          _ -> []
        end

      _ ->
        []
    end
  end

  defp durable_local_codex_os_pid(running_entry) do
    case local_codex_os_pid(running_entry) do
      [os_pid | _] -> os_pid
      [] -> nil
    end
  end

  defp process_cleanup_timeout_ms do
    case Config.settings() do
      {:ok, %{runner: %{process_cleanup_timeout_ms: timeout_ms}}}
      when is_integer(timeout_ms) and timeout_ms > 0 ->
        timeout_ms

      _ ->
        @default_process_cleanup_timeout_ms
    end
  rescue
    _ -> @default_process_cleanup_timeout_ms
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
         %State{running: running, claimed: claimed, holds: holds} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(holds, issue.id) and
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
    issue_routable_to_worker?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable_to_worker?(%Issue{assigned_to_worker: assigned_to_worker})
       when is_boolean(assigned_to_worker),
       do: assigned_to_worker

  defp issue_routable_to_worker?(_issue), do: true

  defp todo_issue_blocked_by_non_terminal?(
         %Issue{state: issue_state, blocked_by: blockers},
         terminal_states
       )
       when is_binary(issue_state) and is_list(blockers) do
    normalize_issue_state(issue_state) == "todo" and
      Enum.any?(blockers, fn
        %{state: blocker_state} when is_binary(blocker_state) ->
          !terminal_issue_state?(blocker_state, terminal_states)

        _ ->
          true
      end)
  end

  defp todo_issue_blocked_by_non_terminal?(_issue, _terminal_states), do: false

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp normalize_issue_state(_state_name), do: ""

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
         preferred_workspace_path \\ nil,
         worker_affinity \\ nil,
         phase_budget \\ nil
       ) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issue_states_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(
          state,
          refreshed_issue,
          attempt,
          preferred_worker_host,
          preferred_workspace_path,
          worker_affinity,
          phase_budget
        )

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        state

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        state

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp do_dispatch_issue(
         %State{} = state,
         issue,
         attempt,
         preferred_worker_host,
         preferred_workspace_path,
         worker_affinity,
         phase_budget
       ) do
    recipient = self()

    case select_worker_host(state, preferred_worker_host) do
      :no_worker_capacity ->
        Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
        state

      worker_host ->
        spawn_issue_on_worker_host(
          state,
          issue,
          attempt,
          recipient,
          worker_host,
          preferred_workspace_path,
          worker_affinity,
          phase_budget
        )
    end
  end

  defp spawn_issue_on_worker_host(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         preferred_workspace_path,
         worker_affinity,
         phase_budget
       ) do
    case Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fn ->
           AgentRunner.run(issue, recipient,
             attempt: attempt,
             worker_host: worker_host,
             workspace_path: preferred_workspace_path,
             resume_phase: phase_budget_value(phase_budget, :phase),
             max_additional_input_tokens: phase_budget_value(phase_budget, :effective_additional_input_tokens),
             continue_after_turn: fn issue_id ->
               continue_after_turn?(recipient, issue_id)
             end
           )
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        claimed_issue = claim_issue_for_dispatch(issue)
        started_at = DateTime.utc_now()
        initial_progress = initial_progress_snapshot(issue, claimed_issue)

        Logger.info("Dispatching issue to agent: #{issue_context(claimed_issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")

        running =
          Map.put(state.running, issue.id, %{
            pid: pid,
            ref: ref,
            identifier: claimed_issue.identifier,
            issue: claimed_issue,
            worker_host: worker_host,
            workspace_path: preferred_workspace_path,
            worker_affinity: worker_affinity,
            session_id: nil,
            last_codex_message: nil,
            last_codex_timestamp: nil,
            last_codex_event: nil,
            progress_phase: initial_progress.phase,
            progress_detail: initial_progress.detail,
            progress_updated_at: started_at,
            codex_app_server_pid: nil,
            codex_app_server_port: nil,
            thread_id: nil,
            turn_id: nil,
            codex_input_tokens: 0,
            codex_output_tokens: 0,
            codex_total_tokens: 0,
            codex_last_reported_input_tokens: 0,
            codex_last_reported_output_tokens: 0,
            codex_last_reported_total_tokens: 0,
            input_token_limit: phase_budget_limit(phase_budget) || Config.input_token_limit_for_issue(claimed_issue),
            input_token_tier_limit: Config.input_token_limit_for_issue(claimed_issue),
            input_token_warning_ratio: Config.settings!().codex.input_token_warning_ratio,
            input_token_checkpoint_grace: Config.settings!().codex.input_token_checkpoint_grace,
            input_token_warning_sent: false,
            input_token_warning_status: nil,
            input_token_warning_threshold: nil,
            input_token_warning_observed_at: nil,
            input_token_warning_ack_timer_ref: nil,
            input_token_warning_ack_token: nil,
            input_token_warning_reader_busy: false,
            resume_phase: phase_budget_value(phase_budget, :phase),
            requested_additional_input_tokens: phase_budget_value(phase_budget, :requested_additional_input_tokens),
            effective_additional_input_tokens: phase_budget_value(phase_budget, :effective_additional_input_tokens),
            attempt_input_token_baseline: 0,
            turn_count: 0,
            retry_attempt: normalize_retry_attempt(attempt),
            started_at: started_at
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

        metadata =
          spawn_failure_retry_metadata(
            issue.identifier,
            reason,
            worker_host,
            preferred_workspace_path,
            worker_affinity,
            phase_budget
          )

        schedule_issue_retry(state, issue.id, next_attempt, metadata)
    end
  end

  defp spawn_failure_retry_metadata(
         identifier,
         reason,
         worker_host,
         workspace_path,
         worker_affinity,
         phase_budget
       ) do
    %{
      identifier: identifier,
      error: "failed to spawn agent: #{inspect(reason)}",
      worker_host: worker_host,
      workspace_path: workspace_path,
      worker_affinity: worker_affinity,
      phase_budget: phase_budget
    }
  end

  @doc false
  @spec spawn_failure_retry_metadata_for_test(
          String.t(),
          term(),
          String.t() | nil,
          String.t() | nil,
          :local | nil,
          map() | nil
        ) :: map()
  def spawn_failure_retry_metadata_for_test(
        identifier,
        reason,
        worker_host,
        workspace_path,
        worker_affinity,
        phase_budget
      ) do
    spawn_failure_retry_metadata(
      identifier,
      reason,
      worker_host,
      workspace_path,
      worker_affinity,
      phase_budget
    )
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
    error = pick_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)
    worker_affinity = pick_retry_worker_affinity(previous_retry, metadata)
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
            error: error,
            worker_host: worker_host,
            workspace_path: workspace_path,
            worker_affinity: worker_affinity,
            phase_budget: phase_budget
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          error: Map.get(retry_entry, :error),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path),
          worker_affinity: Map.get(retry_entry, :worker_affinity),
          phase_budget: Map.get(retry_entry, :phase_budget)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    cond do
      not retry_authorized?(state, issue_id, metadata) ->
        {:noreply, state}

      not state.hold_store_available ->
        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt,
           Map.put(metadata, :error, "durable hold state unavailable")
         )}

      true ->
        do_handle_retry_issue(state, issue_id, attempt, metadata)
    end
  end

  defp do_handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_candidate_issues() do
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

        cleanup_issue_workspace(issue.identifier, metadata[:worker_host])
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

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier, worker_host)
  end

  defp cleanup_issue_workspace(_identifier, _worker_host), do: :ok

  defp run_terminal_workspace_cleanup(holds) when is_map(holds) do
    held_identifiers =
      holds
      |> Map.values()
      |> Enum.map(&Map.get(&1, :identifier))
      |> MapSet.new()

    case Tracker.fetch_issues_by_states(Config.settings!().tracker.terminal_states) do
      {:ok, issues} ->
        Enum.each(issues, &cleanup_terminal_issue_workspace(&1, held_identifiers))

      {:error, reason} ->
        Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
    end
  end

  defp cleanup_terminal_issue_workspace(%Issue{identifier: identifier}, held_identifiers)
       when is_binary(identifier) do
    unless MapSet.member?(held_identifiers, identifier) do
      cleanup_issue_workspace(identifier)
    end
  end

  defp cleanup_terminal_issue_workspace(_issue, _held_identifiers), do: :ok

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         retry_worker_slots_available?(state, metadata) do
      {:noreply,
       dispatch_issue(
         state,
         issue,
         attempt,
         metadata[:worker_host],
         metadata[:workspace_path],
         metadata[:worker_affinity],
         metadata[:phase_budget]
       )}
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
    %{state | claimed: MapSet.delete(state.claimed, issue_id)}
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

  defp continue_after_turn?(server, issue_id) do
    GenServer.call(server, {:continue_after_turn, issue_id}, 5_000)
  catch
    :exit, _ -> false
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
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

  defp pick_retry_worker_affinity(previous_retry, metadata) do
    metadata[:worker_affinity] || Map.get(previous_retry, :worker_affinity)
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

  defp retry_worker_slots_available?(
         %State{} = state,
         %{worker_host: worker_host, workspace_path: workspace_path}
       )
       when is_binary(worker_host) and worker_host != "" and is_binary(workspace_path) and
              workspace_path != "" do
    worker_host in Config.settings!().worker.ssh_hosts and
      worker_host_slots_available?(state, worker_host)
  end

  defp retry_worker_slots_available?(%State{} = state, %{worker_affinity: :local}) do
    Config.settings!().worker.ssh_hosts == [] and worker_slots_available?(state, nil)
  end

  defp retry_worker_slots_available?(%State{} = state, metadata) do
    worker_slots_available?(state, metadata[:worker_host])
  end

  @doc false
  @spec retry_worker_slots_available_for_test(term(), map()) :: boolean()
  def retry_worker_slots_available_for_test(%State{} = state, metadata) when is_map(metadata) do
    retry_worker_slots_available?(state, metadata)
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

  defp call_control(server, message) do
    GenServer.call(server, message, 15_000)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp find_known_issue_id(%State{} = state, issue_identifier) do
    find_issue_id_by_identifier(state.running, issue_identifier) ||
      find_issue_id_by_identifier(state.retry_attempts, issue_identifier) ||
      find_issue_id_by_identifier(state.holds, issue_identifier)
  end

  defp find_hold_by_identifier(holds, issue_identifier) do
    Enum.find(holds, fn {_issue_id, hold} ->
      same_identifier?(Map.get(hold, :identifier), issue_identifier)
    end)
  end

  defp find_issue_id_by_identifier(entries, issue_identifier) do
    Enum.find_value(entries, fn {issue_id, entry} ->
      if same_identifier?(Map.get(entry, :identifier), issue_identifier), do: issue_id
    end)
  end

  defp same_identifier?(left, right) when is_binary(left) and is_binary(right) do
    String.downcase(left) == String.downcase(right)
  end

  defp same_identifier?(_left, _right), do: false

  defp stop_known_issue(%State{} = state, issue_id) do
    cond do
      running_entry = Map.get(state.running, issue_id) ->
        observed = Map.get(running_entry, :codex_input_tokens, 0)
        hold_running_issue(state, issue_id, running_entry, "manual_stop", nil, observed)

      retry_entry = Map.get(state.retry_attempts, issue_id) ->
        stop_retrying_issue(state, issue_id, retry_entry)

      match?(%{cleanup_pending: true}, Map.get(state.holds, issue_id)) ->
        {:error, state, :cleanup_failed}

      hold = Map.get(state.holds, issue_id) ->
        {:ok, state, hold}
    end
  end

  defp stop_retrying_issue(state, issue_id, retry_entry) do
    hold = %{
      issue_id: issue_id,
      identifier: retry_entry.identifier,
      reason: "manual_stop",
      limit: nil,
      observed_tokens: 0,
      issue_state: held_issue_state(issue_id),
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

    case persist_hold_state(held_state) do
      :ok ->
        if is_reference(Map.get(retry_entry, :timer_ref)) do
          Process.cancel_timer(retry_entry.timer_ref)
        end

        {:ok, held_state, hold}

      {:error, reason} ->
        Logger.error("Failed to persist retry hold issue_id=#{issue_id} issue_identifier=#{retry_entry.identifier} reason=#{inspect(reason)}")

        {:error, state, :hold_state_unavailable}
    end
  end

  defp held_issue_state(issue_id) do
    case Tracker.fetch_issue_states_by_ids([issue_id]) do
      {:ok, [%Issue{state: state} | _]} -> state
      _ -> nil
    end
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
    GenServer.call(server, :request_refresh)
  catch
    :exit, _ -> :unavailable
  end

  @spec stop_issue(String.t()) ::
          {:ok, map()} | {:error, :issue_not_found | :unavailable | :cleanup_failed | :hold_state_unavailable}
  def stop_issue(issue_identifier), do: stop_issue(issue_identifier, __MODULE__)

  @spec stop_issue(String.t(), GenServer.server()) ::
          {:ok, map()} | {:error, :issue_not_found | :unavailable | :cleanup_failed | :hold_state_unavailable}
  def stop_issue(issue_identifier, server) when is_binary(issue_identifier) do
    call_control(server, {:stop_issue, issue_identifier})
  end

  @spec resume_issue(String.t()) ::
          {:ok, map()}
          | {:error,
             :issue_not_found
             | :unavailable
             | :cleanup_failed
             | :hold_state_unavailable
             | :resume_phase_required
             | :invalid_resume_phase
             | :max_additional_input_tokens_required
             | :invalid_max_additional_input_tokens
             | :tracker_unavailable}
  def resume_issue(issue_identifier), do: resume_issue(issue_identifier, %{}, __MODULE__)

  @spec resume_issue(String.t(), GenServer.server()) ::
          {:ok, map()} | {:error, atom()}
  def resume_issue(issue_identifier, server) when is_binary(issue_identifier) do
    resume_issue(issue_identifier, %{}, server)
  end

  @spec resume_issue(String.t(), map(), GenServer.server()) ::
          {:ok, map()} | {:error, atom()}
  def resume_issue(issue_identifier, options, server)
      when is_binary(issue_identifier) and is_map(options) do
    call_control(server, {:resume_issue, issue_identifier, options})
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    GenServer.call(server, :snapshot, timeout)
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, _ -> :unavailable
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
          state: metadata.issue.state,
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          progress_phase: Map.get(metadata, :progress_phase),
          progress_detail: Map.get(metadata, :progress_detail),
          progress_updated_at: Map.get(metadata, :progress_updated_at),
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          input_token_limit: Map.get(metadata, :input_token_limit),
          input_token_tier_limit: Map.get(metadata, :input_token_tier_limit),
          input_token_warning_ratio: Map.get(metadata, :input_token_warning_ratio),
          input_token_warning_status: Map.get(metadata, :input_token_warning_status),
          input_token_warning_threshold: Map.get(metadata, :input_token_warning_threshold),
          input_token_warning_observed_at: Map.get(metadata, :input_token_warning_observed_at),
          input_token_checkpoint_grace: Map.get(metadata, :input_token_checkpoint_grace),
          resume_phase: Map.get(metadata, :resume_phase),
          requested_additional_input_tokens: Map.get(metadata, :requested_additional_input_tokens),
          effective_additional_input_tokens: Map.get(metadata, :effective_additional_input_tokens),
          attempt_input_token_baseline: Map.get(metadata, :attempt_input_token_baseline, 0),
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
      end)

    held =
      state.holds
      |> Enum.map(fn {_issue_id, hold} ->
        %{
          issue_id: hold.issue_id,
          identifier: hold.identifier,
          reason: hold.reason,
          limit: hold.limit,
          observed_tokens: hold.observed_tokens,
          warning_threshold: Map.get(hold, :warning_threshold),
          warning_observed_at: Map.get(hold, :warning_observed_at),
          checkpoint_grace: Map.get(hold, :checkpoint_grace),
          resume_phase: Map.get(hold, :resume_phase),
          requested_additional_input_tokens: Map.get(hold, :requested_additional_input_tokens),
          effective_additional_input_tokens: Map.get(hold, :effective_additional_input_tokens),
          attempt_input_token_baseline: Map.get(hold, :attempt_input_token_baseline, 0),
          issue_state: hold.issue_state,
          worker_host: Map.get(hold, :worker_host),
          workspace_path: Map.get(hold, :workspace_path),
          cleanup_pending: Map.get(hold, :cleanup_pending, false),
          held_at: hold.held_at
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path)
        }
      end)

    {:reply,
     %{
       running: running,
       retrying: retrying,
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

  def handle_call({:continue_after_turn, issue_id}, _from, state) do
    continue? =
      case Map.get(state.running, issue_id) do
        %{input_token_warning_status: "delivered"} -> false
        %{input_token_warning_status: "unsupported"} -> false
        nil -> false
        _running_entry -> true
      end

    {:reply, continue?, state}
  end

  def handle_call({:resume_issue, issue_identifier}, _from, state) do
    handle_resume_call(issue_identifier, %{}, state)
  end

  def handle_call({:resume_issue, issue_identifier, options}, _from, state) do
    handle_resume_call(issue_identifier, options, state)
  end

  defp handle_resume_call(issue_identifier, options, state) do
    case find_hold_by_identifier(state.holds, issue_identifier) do
      nil ->
        {:reply, {:error, :issue_not_found}, state}

      {issue_id, hold} ->
        case authorize_hold_resume(issue_id, hold, options) do
          {:ok, phase_budget} -> resume_held_issue(state, issue_id, hold, phase_budget)
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  defp authorize_hold_resume(issue_id, hold, options) do
    if budget_hold?(hold) do
      authorize_budget_hold_resume(issue_id, options)
    else
      {:ok, nil}
    end
  end

  defp authorize_budget_hold_resume(issue_id, options) do
    with {:ok, phase} <- validate_resume_phase(option_value(options, :phase)),
         {:ok, requested} <-
           validate_resume_allowance(option_value(options, :max_additional_input_tokens)),
         {:ok, tier_limit} <- current_issue_tier_limit(issue_id) do
      effective = if is_integer(tier_limit), do: min(requested, tier_limit), else: requested

      {:ok,
       %{
         phase: phase,
         requested_additional_input_tokens: requested,
         effective_additional_input_tokens: effective,
         attempt_input_token_baseline: 0,
         current_issue_tier_limit: tier_limit
       }}
    end
  end

  defp budget_hold?(hold), do: Map.get(hold, :reason) in @budget_hold_reasons

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
    case Tracker.fetch_issue_states_by_ids([issue_id]) do
      {:ok, [%Issue{} = issue | _]} -> {:ok, Config.input_token_limit_for_issue(issue)}
      _ -> {:error, :tracker_unavailable}
    end
  end

  defp option_value(options, key) do
    Map.get(options, key, Map.get(options, Atom.to_string(key)))
  end

  defp resume_held_issue(%State{running: running} = state, issue_id, _hold, _phase_budget)
       when is_map_key(running, issue_id) do
    {:reply, {:error, :cleanup_failed}, state}
  end

  defp resume_held_issue(state, issue_id, %{cleanup_pending: true} = hold, phase_budget) do
    case terminate_codex_process_tree(hold) do
      :ok ->
        case mark_hold_cleanup_complete(state, issue_id, hold) do
          {:ok, cleaned_state, cleaned_hold} ->
            resume_cleaned_hold(cleaned_state, issue_id, cleaned_hold, phase_budget)

          {:error, safe_state, reason} ->
            {:reply, {:error, reason}, schedule_hold_state_persist_retry(safe_state)}
        end

      {:error, reason} ->
        Logger.error("Failed to complete pending hold cleanup issue_id=#{issue_id} reason=#{inspect(reason)}; keeping hold")
        {:reply, {:error, :cleanup_failed}, state}
    end
  end

  defp resume_held_issue(state, issue_id, hold, phase_budget) do
    resume_cleaned_hold(state, issue_id, hold, phase_budget)
  end

  defp resume_cleaned_hold(state, issue_id, hold, phase_budget) do
    case phase_budget do
      nil ->
        state
        |> release_issue_hold(issue_id)
        |> finish_hold_resume(issue_id, hold, nil)

      %{} ->
        persist_phase_resume_authorization(state, issue_id, hold, phase_budget)
    end
  end

  defp persist_phase_resume_authorization(state, issue_id, hold, phase_budget) do
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

    case persist_hold_state(authorized_state) do
      :ok -> finish_phase_resume(authorized_state, issue_id, pending_hold, phase_budget)
      {:error, _reason} -> {:reply, {:error, :hold_state_unavailable}, state}
    end
  end

  defp finish_hold_resume(%State{holds: holds} = state, issue_id, _hold, _phase_budget)
       when is_map_key(holds, issue_id) do
    {:reply, {:error, :hold_state_unavailable}, state}
  end

  defp finish_hold_resume(state, issue_id, hold, phase_budget) do
    updated_state =
      state
      |> Map.update!(:claimed, &MapSet.put(&1, issue_id))
      |> schedule_issue_retry(issue_id, 1, %{
        identifier: hold.identifier,
        worker_host: Map.get(hold, :worker_host),
        workspace_path: Map.get(hold, :workspace_path),
        worker_affinity: if(is_nil(Map.get(hold, :worker_host)), do: :local),
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
      |> Map.merge(phase_budget || %{})

    {:reply, {:ok, receipt}, updated_state}
  end

  defp finish_phase_resume(state, issue_id, hold, phase_budget) do
    updated_state =
      do_schedule_issue_retry(state, issue_id, 1, %{
        identifier: hold.identifier,
        worker_host: Map.get(hold, :worker_host),
        workspace_path: Map.get(hold, :workspace_path),
        worker_affinity: if(is_nil(Map.get(hold, :worker_host)), do: :local),
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

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    summarized_update = summarize_codex_update(update)
    progress_update = StatusDashboard.codex_progress_update(summarized_update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarized_update,
        session_id: session_id_for_update(running_entry.session_id, update),
        thread_id: runtime_id_for_update(Map.get(running_entry, :thread_id), update, :thread_id),
        turn_id: runtime_id_for_update(Map.get(running_entry, :turn_id), update, :turn_id),
        last_codex_event: event,
        progress_phase: Map.get(progress_update || %{}, :phase, Map.get(running_entry, :progress_phase)),
        progress_detail: Map.get(progress_update || %{}, :detail, Map.get(running_entry, :progress_detail)),
        progress_updated_at: Map.get(progress_update || %{}, :updated_at, Map.get(running_entry, :progress_updated_at)),
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        input_token_warning_status: warning_status_for_update(Map.get(running_entry, :input_token_warning_status), event),
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

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp runtime_id_for_update(existing, update, key) do
    case Map.get(update, key) do
      value when is_binary(value) -> value
      _ -> existing
    end
  end

  defp warning_status_for_update(_existing, :token_budget_warning_delivered), do: "delivered"
  defp warning_status_for_update(_existing, :token_budget_warning_unsupported), do: "unsupported"
  defp warning_status_for_update(existing, _event), do: existing

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

  defp claim_issue_for_dispatch(issue, issue_state_updater \\ &Tracker.update_issue_state/2)

  defp claim_issue_for_dispatch(%Issue{id: issue_id, state: state_name} = issue, issue_state_updater)
       when is_binary(issue_id) and is_binary(state_name) and is_function(issue_state_updater, 2) do
    if normalize_issue_state(state_name) == "todo" do
      case issue_state_updater.(issue_id, "In Progress") do
        :ok ->
          %{issue | state: "In Progress"}

        {:error, reason} ->
          Logger.warning("Failed to move issue to In Progress before agent start: #{issue_context(issue)} reason=#{inspect(reason)}")
          issue
      end
    else
      issue
    end
  end

  defp claim_issue_for_dispatch(%Issue{} = issue, _issue_state_updater), do: issue

  defp initial_progress_snapshot(%Issue{state: original_state}, %Issue{state: claimed_state})
       when is_binary(original_state) and is_binary(claimed_state) do
    cond do
      normalize_issue_state(original_state) == "todo" and normalize_issue_state(claimed_state) == "in progress" ->
        %{phase: "Claimed", detail: "Moved to In Progress; preparing workspace"}

      normalize_issue_state(claimed_state) == "in progress" ->
        %{phase: "Resuming", detail: "Preparing existing In Progress workspace"}

      true ->
        %{phase: "Claimed", detail: "Preparing workspace"}
    end
  end

  defp initial_progress_snapshot(_original_issue, _claimed_issue) do
    %{phase: "Claimed", detail: "Preparing workspace"}
  end

  defp maybe_refresh_progress_for_runtime_info(running_entry, runtime_info) when is_map(runtime_info) do
    workspace_path = Map.get(runtime_info, :workspace_path)

    cond do
      not is_binary(workspace_path) or workspace_path == "" ->
        running_entry

      Map.get(running_entry, :last_codex_message) != nil ->
        running_entry

      true ->
        Map.merge(running_entry, %{
          progress_phase: "Bootstrapping",
          progress_detail: "Workspace #{Path.basename(workspace_path)} ready; starting Codex",
          progress_updated_at: DateTime.utc_now()
        })
    end
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
    candidate_issue?(issue, active_state_set(), terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states)
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
    update
    |> rate_limit_payload_candidates()
    |> Enum.find_value(&rate_limits_from_payload/1)
  end

  defp rate_limit_payload_candidates(update) do
    [
      update[:rate_limits],
      update[:rateLimits],
      Map.get(update, "rate_limits"),
      Map.get(update, "rateLimits"),
      Map.get(update, :rate_limits),
      Map.get(update, :rateLimits),
      update[:payload],
      update[:details],
      Map.get(update, "payload"),
      Map.get(update, "details"),
      update
    ]
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
    direct =
      Map.get(payload, "rate_limits") ||
        Map.get(payload, :rate_limits) ||
        Map.get(payload, "rateLimits") ||
        Map.get(payload, :rateLimits)

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
    has_buckets =
      Enum.any?(
        [
          "primary",
          :primary,
          "secondary",
          :secondary,
          "credits",
          :credits,
          "rateLimits",
          :rateLimits,
          "rate_limits",
          :rate_limits
        ],
        &Map.has_key?(payload, &1)
      )

    has_buckets
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
