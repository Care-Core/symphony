defmodule SymphonyElixirWeb.Presenter do
  @moduledoc """
  Shared projections for the observability API and dashboard.
  """

  alias SymphonyElixir.{Config, Orchestrator, StatusDashboard, Workspace}

  @spec state_payload(GenServer.name(), timeout()) :: map()
  def state_payload(orchestrator, snapshot_timeout_ms) do
    generated_at = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        %{
          generated_at: generated_at,
          counts: %{
            running: length(snapshot.running),
            deferred: length(Map.get(snapshot, :deferred, [])),
            retrying: length(snapshot.retrying),
            blocked: length(Map.get(snapshot, :blocked, [])),
            held: length(Map.get(snapshot, :held, []))
          },
          running: Enum.map(snapshot.running, &running_entry_payload/1),
          deferred: Enum.map(Map.get(snapshot, :deferred, []), &deferred_entry_payload/1),
          retrying: Enum.map(snapshot.retrying, &retry_entry_payload/1),
          blocked: Enum.map(Map.get(snapshot, :blocked, []), &blocked_entry_payload/1),
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
        deferred = Enum.find(Map.get(snapshot, :deferred, []), &(&1.identifier == issue_identifier))
        retry = Enum.find(snapshot.retrying, &(&1.identifier == issue_identifier))
        blocked = Enum.find(Map.get(snapshot, :blocked, []), &(&1.identifier == issue_identifier))
        hold = Enum.find(Map.get(snapshot, :held, []), &(&1.identifier == issue_identifier))

        if is_nil(running) and is_nil(deferred) and is_nil(retry) and is_nil(blocked) and is_nil(hold) do
          {:error, :issue_not_found}
        else
          {:ok, issue_payload_body(issue_identifier, running, deferred, retry, blocked, hold)}
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

  @spec stop_payload(String.t(), GenServer.name()) :: {:ok, map()} | {:error, atom()}
  def stop_payload(issue_identifier, orchestrator) do
    case Orchestrator.stop_issue(issue_identifier, orchestrator) do
      {:ok, hold} ->
        {:ok,
         %{
           issue_id: hold.issue_id,
           issue_identifier: hold.identifier,
           status: "held",
           hold: held_entry_payload(hold)
         }}

      error ->
        error
    end
  end

  @spec resume_payload(String.t(), map(), GenServer.name()) :: {:ok, map()} | {:error, atom()}
  def resume_payload(issue_identifier, options, orchestrator) do
    case Orchestrator.resume_issue(issue_identifier, options, orchestrator) do
      {:ok, result} ->
        {:ok,
         %{
           issue_id: result.issue_id,
           issue_identifier: result.identifier,
           status: "resumed",
           hold: nil,
           resume_phase: result.phase,
           requested_additional_input_tokens: result.requested_additional_input_tokens,
           effective_additional_input_tokens: result.effective_additional_input_tokens,
           current_issue_tier_limit: result.current_issue_tier_limit,
           attempt_input_token_baseline: Map.get(result, :attempt_input_token_baseline, 0),
           workspace_path: Map.get(result, :workspace_path)
         }}

      error ->
        error
    end
  end

  @spec progress_payload(String.t(), map(), GenServer.name()) :: {:ok, map()} | {:error, atom()}
  def progress_payload(issue_identifier, attributes, orchestrator) do
    Orchestrator.record_progress(issue_identifier, attributes, orchestrator)
  end

  @spec review_payload(String.t(), map(), GenServer.name()) :: {:ok, map()} | {:error, atom()}
  def review_payload(issue_identifier, attributes, orchestrator) do
    Orchestrator.authorize_review(issue_identifier, attributes, orchestrator)
  end

  @spec wait_payload(String.t(), map(), GenServer.name()) :: {:ok, map()} | {:error, atom()}
  def wait_payload(issue_identifier, attributes, orchestrator) do
    Orchestrator.register_deferred_wait(issue_identifier, attributes, orchestrator)
  end

  defp issue_payload_body(issue_identifier, running, deferred, retry, blocked, hold) do
    entries = [running, deferred, retry, blocked, hold]

    %{
      issue_identifier: issue_identifier,
      issue_id: first_entry_value(entries, :issue_id),
      status: issue_status(running, deferred, retry, blocked, hold),
      workspace: %{
        path: workspace_path(issue_identifier, entries),
        host: workspace_host(entries)
      },
      attempts: %{
        restart_count: restart_count(retry),
        current_retry_attempt: retry_attempt(retry)
      },
      running: optional_payload(running, &running_issue_payload/1),
      deferred: optional_payload(deferred, &deferred_issue_payload/1),
      retry: optional_payload(retry, &retry_issue_payload/1),
      blocked: optional_payload(blocked, &blocked_issue_payload/1),
      hold: optional_payload(hold, &held_entry_payload/1),
      logs: %{
        codex_session_logs: []
      },
      recent_events: recent_events_payload(first_entry([running, blocked])),
      last_error: first_entry_value([blocked, retry], :error),
      tracked: %{}
    }
  end

  defp first_entry(entries), do: Enum.find(entries, &(not is_nil(&1)))

  defp first_entry_value(entries, key), do: Enum.find_value(entries, &entry_value(&1, key))
  defp entry_value(nil, _key), do: nil
  defp entry_value(entry, key), do: Map.get(entry, key)

  defp optional_payload(nil, _mapper), do: nil
  defp optional_payload(entry, mapper), do: mapper.(entry)

  defp restart_count(retry), do: max(retry_attempt(retry) - 1, 0)
  defp retry_attempt(nil), do: 0
  defp retry_attempt(retry), do: retry.attempt || 0

  defp issue_status(_running, _deferred, _retry, _blocked, hold) when not is_nil(hold), do: "held"
  defp issue_status(running, _deferred, _retry, _blocked, nil) when not is_nil(running), do: "running"
  defp issue_status(nil, deferred, _retry, _blocked, nil) when not is_nil(deferred), do: "deferred_wait"
  defp issue_status(nil, nil, retry, _blocked, nil) when not is_nil(retry), do: "retrying"
  defp issue_status(nil, nil, nil, _blocked, nil), do: "blocked"

  defp running_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      issue_url: Map.get(entry, :issue_url),
      state: entry.state,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: entry.session_id,
      turn_count: Map.get(entry, :turn_count, 0),
      last_event: entry.last_codex_event,
      last_message: summarize_message(entry.last_codex_message),
      started_at: iso8601(entry.started_at),
      last_event_at: iso8601(entry.last_codex_timestamp),
      tokens: %{
        input_tokens: entry.codex_input_tokens,
        output_tokens: entry.codex_output_tokens,
        total_tokens: entry.codex_total_tokens
      }
    }
  end

  defp deferred_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      state: entry.state,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      started_at: iso8601(Map.get(entry, :started_at))
    }
  end

  defp retry_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      issue_url: Map.get(entry, :issue_url),
      attempt: entry.attempt,
      due_at: due_at_iso8601(entry.due_in_ms),
      error: entry.error,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path)
    }
  end

  defp blocked_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      issue_url: Map.get(entry, :issue_url),
      state: entry.state,
      error: entry.error,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: entry.session_id,
      blocked_at: iso8601(entry.blocked_at),
      last_event: entry.last_codex_event,
      last_message: summarize_message(entry.last_codex_message),
      last_event_at: iso8601(entry.last_codex_timestamp)
    }
  end

  defp held_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      reason: entry.reason,
      limit: entry.limit,
      observed_tokens: entry.observed_tokens,
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
      last_event_at: iso8601(running.last_codex_timestamp),
      tokens: %{
        input_tokens: running.codex_input_tokens,
        output_tokens: running.codex_output_tokens,
        total_tokens: running.codex_total_tokens
      }
    }
  end

  defp deferred_issue_payload(deferred) do
    %{
      state: deferred.state,
      worker_host: Map.get(deferred, :worker_host),
      workspace_path: Map.get(deferred, :workspace_path),
      started_at: iso8601(Map.get(deferred, :started_at))
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

  defp blocked_issue_payload(blocked) do
    %{
      worker_host: Map.get(blocked, :worker_host),
      workspace_path: Map.get(blocked, :workspace_path),
      session_id: blocked.session_id,
      state: blocked.state,
      error: blocked.error,
      blocked_at: iso8601(blocked.blocked_at),
      last_event: blocked.last_codex_event,
      last_message: summarize_message(blocked.last_codex_message),
      last_event_at: iso8601(blocked.last_codex_timestamp)
    }
  end

  defp workspace_path(issue_identifier, entries) do
    first_entry_value(entries, :workspace_path) ||
      Path.join(Config.settings!().workspace.root, Workspace.workspace_key(issue_identifier))
  end

  defp workspace_host(entries), do: first_entry_value(entries, :worker_host)

  defp recent_events_payload(nil), do: []

  defp recent_events_payload(entry) do
    [
      %{
        at: iso8601(entry.last_codex_timestamp),
        event: entry.last_codex_event,
        message: summarize_message(entry.last_codex_message)
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
