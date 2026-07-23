defmodule SymphonyElixirWeb.Presenter do
  @moduledoc """
  Shared projections for the observability API and dashboard.
  """

  alias SymphonyElixir.{Config, Orchestrator, StatusDashboard}

  @spec state_payload(GenServer.name(), timeout()) :: map()
  def state_payload(orchestrator, snapshot_timeout_ms) do
    generated_at = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        %{
          generated_at: generated_at,
          counts: %{
            running: length(snapshot.running),
            retrying: length(snapshot.retrying),
            held: length(Map.get(snapshot, :held, []))
          },
          running: Enum.map(snapshot.running, &running_entry_payload/1),
          retrying: Enum.map(snapshot.retrying, &retry_entry_payload/1),
          held: Enum.map(Map.get(snapshot, :held, []), &held_entry_payload/1),
          codex_totals: snapshot.codex_totals,
          rate_limits: snapshot.rate_limits
        }

      :timeout ->
        %{generated_at: generated_at, error: %{code: "snapshot_timeout", message: "Snapshot timed out"}}

      :unavailable ->
        %{generated_at: generated_at, error: %{code: "snapshot_unavailable", message: "Snapshot unavailable"}}
    end
  end

  @spec issue_payload(String.t(), GenServer.name(), timeout()) :: {:ok, map()} | {:error, :issue_not_found}
  def issue_payload(issue_identifier, orchestrator, snapshot_timeout_ms) when is_binary(issue_identifier) do
    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        running = Enum.find(snapshot.running, &(&1.identifier == issue_identifier))
        retry = Enum.find(snapshot.retrying, &(&1.identifier == issue_identifier))
        hold = Enum.find(Map.get(snapshot, :held, []), &(&1.identifier == issue_identifier))

        if is_nil(running) and is_nil(retry) and is_nil(hold) do
          {:error, :issue_not_found}
        else
          {:ok, issue_payload_body(issue_identifier, running, retry, hold)}
        end

      _ ->
        {:error, :issue_not_found}
    end
  end

  @spec refresh_payload(GenServer.name()) :: {:ok, map()} | {:error, :unavailable}
  def refresh_payload(orchestrator) do
    case Orchestrator.request_refresh(orchestrator) do
      :unavailable ->
        {:error, :unavailable}

      payload ->
        {:ok, Map.update!(payload, :requested_at, &DateTime.to_iso8601/1)}
    end
  end

  @spec stop_payload(String.t(), GenServer.name()) ::
          {:ok, map()}
          | {:error, :issue_not_found | :unavailable | :cleanup_failed | :hold_state_unavailable}
  def stop_payload(issue_identifier, orchestrator) do
    case Orchestrator.stop_issue(issue_identifier, orchestrator) do
      {:ok, hold} -> {:ok, control_payload(hold, false)}
      error -> error
    end
  end

  @spec resume_payload(String.t(), map(), GenServer.name()) ::
          {:ok, map()}
          | {:error, atom()}
  def resume_payload(issue_identifier, options, orchestrator) do
    case Orchestrator.resume_issue(issue_identifier, options, orchestrator) do
      {:ok, result} -> {:ok, control_payload(result, true)}
      error -> error
    end
  end

  defp issue_payload_body(issue_identifier, running, retry, hold) do
    %{
      issue_identifier: issue_identifier,
      issue_id: issue_id_from_entries(running, retry, hold),
      status: issue_status(running, retry, hold),
      workspace: %{
        path: workspace_path(issue_identifier, running, retry, hold),
        host: workspace_host(running, retry, hold)
      },
      attempts: %{
        restart_count: restart_count(retry),
        current_retry_attempt: retry_attempt(retry)
      },
      running: running && running_issue_payload(running),
      retry: retry && retry_issue_payload(retry),
      hold: hold && held_entry_payload(hold),
      logs: %{
        codex_session_logs: []
      },
      recent_events: (running && recent_events_payload(running)) || [],
      last_error: retry && retry.error,
      tracked: %{}
    }
  end

  defp issue_id_from_entries(running, retry, hold),
    do: (running && running.issue_id) || (retry && retry.issue_id) || (hold && hold.issue_id)

  defp restart_count(retry), do: max(retry_attempt(retry) - 1, 0)
  defp retry_attempt(nil), do: 0
  defp retry_attempt(retry), do: retry.attempt || 0

  defp issue_status(running, _retry, %{reason: "input_token_resume_pending"})
       when not is_nil(running),
       do: "running"

  defp issue_status(nil, retry, %{reason: "input_token_resume_pending"})
       when not is_nil(retry),
       do: "retrying"

  defp issue_status(_running, _retry, hold) when not is_nil(hold), do: "held"
  defp issue_status(_running, nil, nil), do: "running"
  defp issue_status(nil, _retry, nil), do: "retrying"
  defp issue_status(_running, _retry, nil), do: "running"

  defp running_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      state: entry.state,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: entry.session_id,
      turn_count: Map.get(entry, :turn_count, 0),
      last_event: entry.last_codex_event,
      last_message: summarize_message(entry.last_codex_message),
      progress_phase: Map.get(entry, :progress_phase),
      progress_detail: Map.get(entry, :progress_detail),
      progress_updated_at: iso8601(Map.get(entry, :progress_updated_at)),
      started_at: iso8601(entry.started_at),
      last_event_at: iso8601(entry.last_codex_timestamp),
      tokens: %{
        input_tokens: entry.codex_input_tokens,
        output_tokens: entry.codex_output_tokens,
        total_tokens: entry.codex_total_tokens
      },
      input_token_budget: input_token_budget(entry)
    }
  end

  defp retry_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      attempt: entry.attempt,
      due_at: due_at_iso8601(entry.due_in_ms),
      error: entry.error,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path)
    }
  end

  defp held_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      reason: entry.reason,
      limit: entry.limit,
      observed_tokens: entry.observed_tokens,
      warning_threshold: Map.get(entry, :warning_threshold),
      warning_observed_at: Map.get(entry, :warning_observed_at),
      checkpoint_grace: Map.get(entry, :checkpoint_grace),
      checkpoint_grace_consumed:
        checkpoint_grace_consumed(
          entry.observed_tokens,
          Map.get(entry, :warning_observed_at)
        ),
      resume_phase: Map.get(entry, :resume_phase),
      requested_additional_input_tokens: Map.get(entry, :requested_additional_input_tokens),
      effective_additional_input_tokens: Map.get(entry, :effective_additional_input_tokens),
      attempt_input_token_baseline: Map.get(entry, :attempt_input_token_baseline, 0),
      current_attempt_input_tokens:
        current_attempt_input_tokens(
          entry.observed_tokens,
          Map.get(entry, :attempt_input_token_baseline, 0)
        ),
      issue_state: entry.issue_state,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      cleanup_pending: Map.get(entry, :cleanup_pending, false),
      held_at: iso8601(Map.get(entry, :held_at))
    }
  end

  defp running_issue_payload(running) do
    %{
      worker_host: Map.get(running, :worker_host),
      workspace_path: Map.get(running, :workspace_path),
      session_id: running.session_id,
      turn_count: Map.get(running, :turn_count, 0),
      state: running.state,
      started_at: iso8601(running.started_at),
      last_event: running.last_codex_event,
      last_message: summarize_message(running.last_codex_message),
      progress_phase: Map.get(running, :progress_phase),
      progress_detail: Map.get(running, :progress_detail),
      progress_updated_at: iso8601(Map.get(running, :progress_updated_at)),
      last_event_at: iso8601(running.last_codex_timestamp),
      tokens: %{
        input_tokens: running.codex_input_tokens,
        output_tokens: running.codex_output_tokens,
        total_tokens: running.codex_total_tokens
      },
      input_token_budget: input_token_budget(running)
    }
  end

  defp retry_issue_payload(retry) do
    %{
      attempt: retry.attempt,
      due_at: due_at_iso8601(retry.due_in_ms),
      error: retry.error,
      worker_host: Map.get(retry, :worker_host),
      workspace_path: Map.get(retry, :workspace_path)
    }
  end

  defp workspace_path(issue_identifier, running, retry, hold) do
    (running && Map.get(running, :workspace_path)) ||
      (retry && Map.get(retry, :workspace_path)) ||
      (hold && Map.get(hold, :workspace_path)) ||
      Path.join(Config.settings!().workspace.root, issue_identifier)
  end

  defp workspace_host(running, retry, hold) do
    (running && Map.get(running, :worker_host)) ||
      (retry && Map.get(retry, :worker_host)) ||
      (hold && Map.get(hold, :worker_host))
  end

  defp input_token_budget(entry) do
    observed = Map.get(entry, :codex_input_tokens, 0)
    warning_observed_at = Map.get(entry, :input_token_warning_observed_at)
    attempt_baseline = Map.get(entry, :attempt_input_token_baseline, 0)

    %{
      limit: Map.get(entry, :input_token_limit),
      tier_limit: Map.get(entry, :input_token_tier_limit),
      observed_tokens: observed,
      current_attempt_input_tokens: current_attempt_input_tokens(observed, attempt_baseline),
      attempt_input_token_baseline: attempt_baseline,
      warning_ratio: Map.get(entry, :input_token_warning_ratio),
      warning_threshold: Map.get(entry, :input_token_warning_threshold),
      warning_status: Map.get(entry, :input_token_warning_status),
      warning_observed_at: warning_observed_at,
      checkpoint_grace: Map.get(entry, :input_token_checkpoint_grace),
      checkpoint_grace_consumed: checkpoint_grace_consumed(observed, warning_observed_at),
      resume_phase: Map.get(entry, :resume_phase),
      requested_additional_input_tokens: Map.get(entry, :requested_additional_input_tokens),
      effective_additional_input_tokens: Map.get(entry, :effective_additional_input_tokens)
    }
  end

  defp control_payload(result, resumed) do
    payload = %{
      issue_id: result.issue_id,
      issue_identifier: result.identifier,
      status: if(resumed, do: "resumed", else: "held"),
      hold: if(resumed, do: nil, else: held_entry_payload(result))
    }

    if resumed and is_binary(Map.get(result, :phase)) do
      Map.merge(payload, %{
        resume_phase: Map.get(result, :phase),
        requested_additional_input_tokens: Map.get(result, :requested_additional_input_tokens),
        effective_additional_input_tokens: Map.get(result, :effective_additional_input_tokens),
        current_issue_tier_limit: Map.get(result, :current_issue_tier_limit),
        attempt_input_token_baseline: Map.get(result, :attempt_input_token_baseline, 0),
        workspace_path: Map.get(result, :workspace_path)
      })
    else
      payload
    end
  end

  defp checkpoint_grace_consumed(observed, baseline)
       when is_integer(observed) and is_integer(baseline),
       do: max(observed - baseline, 0)

  defp checkpoint_grace_consumed(_observed, _baseline), do: 0

  defp current_attempt_input_tokens(observed, baseline)
       when is_integer(observed) and is_integer(baseline),
       do: max(observed - baseline, 0)

  defp current_attempt_input_tokens(_observed, _baseline), do: 0

  defp recent_events_payload(running) do
    [
      %{
        at: iso8601(running.last_codex_timestamp),
        event: running.last_codex_event,
        message: summarize_message(running.last_codex_message),
        progress_phase: Map.get(running, :progress_phase),
        progress_detail: Map.get(running, :progress_detail)
      }
    ]
    |> Enum.reject(&is_nil(&1.at))
  end

  defp summarize_message(nil), do: nil
  defp summarize_message(message), do: StatusDashboard.humanize_codex_message(message)

  defp due_at_iso8601(due_in_ms) when is_integer(due_in_ms) do
    DateTime.utc_now()
    |> DateTime.add(div(due_in_ms, 1_000), :second)
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp due_at_iso8601(_due_in_ms), do: nil

  defp iso8601(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp iso8601(_datetime), do: nil
end
