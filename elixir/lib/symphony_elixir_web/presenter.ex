defmodule SymphonyElixirWeb.Presenter do
  @moduledoc """
  Shared projections for the observability API and dashboard.
  """

  alias SymphonyElixir.{Config, Orchestrator, RunArchive, StatusDashboard}

  @spec state_payload(GenServer.name(), timeout()) :: map()
  def state_payload(orchestrator, snapshot_timeout_ms) do
    generated_at = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        sorted_running = sort_running_entries(snapshot.running)
        recent_outcomes = merge_recent_outcomes(Map.get(snapshot, :recent_outcomes, []), RunArchive.recent_outcomes(50))

        %{
          generated_at: generated_at,
          counts: %{
            running: length(sorted_running),
            retrying: length(snapshot.retrying)
          },
          alerts: alerts_payload(snapshot),
          running: Enum.map(sorted_running, &running_entry_payload/1),
          retrying: Enum.map(snapshot.retrying, &retry_entry_payload/1),
          recent_outcomes: Enum.map(recent_outcomes, &recent_outcome_payload/1),
          codex_totals: snapshot.codex_totals,
          rate_limits: snapshot.rate_limits,
          polling: snapshot.polling || %{checking?: false, next_poll_in_ms: nil, poll_interval_ms: nil}
        }

      :timeout ->
        %{generated_at: generated_at, error: %{code: "snapshot_timeout", message: "Snapshot timed out"}}

      :unavailable ->
        %{generated_at: generated_at, error: %{code: "snapshot_unavailable", message: "Snapshot unavailable"}}
    end
  end

  @spec issue_payload(String.t(), GenServer.name(), timeout()) ::
          {:ok, map()} | {:error, :issue_not_found | :snapshot_timeout | :snapshot_unavailable}
  def issue_payload(issue_identifier, orchestrator, snapshot_timeout_ms) when is_binary(issue_identifier) do
    case fetch_issue_entries(issue_identifier, orchestrator, snapshot_timeout_ms) do
      {:ok, running, retry, recent_outcome} -> {:ok, issue_payload_body(issue_identifier, running, retry, recent_outcome)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec run_payload(String.t(), GenServer.name(), timeout()) ::
          {:ok, map()} | {:error, :issue_not_found | :snapshot_timeout | :snapshot_unavailable}
  def run_payload(issue_identifier, orchestrator, snapshot_timeout_ms) when is_binary(issue_identifier) do
    case fetch_issue_entries(issue_identifier, orchestrator, snapshot_timeout_ms) do
      {:ok, running, retry, recent_outcome} ->
        {:ok,
         issue_identifier
         |> issue_payload_body(running, retry, recent_outcome)
         |> Map.put(:inspector_path, "/runs/#{issue_identifier}")}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec events_payload(String.t(), map(), GenServer.name(), timeout()) ::
          {:ok, map()} | {:error, :issue_not_found | :snapshot_timeout | :snapshot_unavailable}
  def events_payload(issue_identifier, params, orchestrator, snapshot_timeout_ms)
      when is_binary(issue_identifier) and is_map(params) do
    case fetch_issue_entries(issue_identifier, orchestrator, snapshot_timeout_ms) do
      {:ok, running, retry, recent_outcome} ->
        events =
          running
          |> event_source(recent_outcome)
          |> raw_recent_events_payload()
          |> sanitize_recent_events_payload()
          |> filter_events(params)
          |> maybe_limit_events(params)

        {:ok,
         %{
           issue_identifier: issue_identifier,
           issue_id: issue_id_from_entries(running, retry, recent_outcome),
           status: issue_status(running, retry, recent_outcome),
           events: events,
           next_cursor: nil
         }}

      {:error, reason} ->
        {:error, reason}
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

  @spec operator_transcript([map()]) :: [map()]
  def operator_transcript(events) when is_list(events), do: operator_transcript_payload(events)
  def operator_transcript(_events), do: []

  defp fetch_issue_entries(issue_identifier, orchestrator, snapshot_timeout_ms) do
    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        running = Enum.find(snapshot.running, &(&1.identifier == issue_identifier))
        retry = Enum.find(snapshot.retrying, &(&1.identifier == issue_identifier))
        recent_outcome = Enum.find(Map.get(snapshot, :recent_outcomes, []), &(Map.get(&1, :identifier) == issue_identifier))

        cond do
          not is_nil(running) or not is_nil(retry) or not is_nil(recent_outcome) ->
            {:ok, running, retry, recent_outcome}

          true ->
            case RunArchive.latest_issue(issue_identifier) do
              {:ok, archived_recent_outcome} -> {:ok, nil, nil, archived_recent_outcome}
              {:error, _reason} -> {:error, :issue_not_found}
            end
        end

      :timeout ->
        {:error, :snapshot_timeout}

      :unavailable ->
        {:error, :snapshot_unavailable}
    end
  end

  defp issue_payload_body(issue_identifier, running, retry, recent_outcome) do
    recent_events =
      running
      |> event_source(recent_outcome)
      |> recent_events_payload()

    %{
      issue_identifier: issue_identifier,
      issue_id: issue_id_from_entries(running, retry, recent_outcome),
      issue_url: issue_url_from_entries(running, retry, recent_outcome),
      title: issue_title(running, retry, recent_outcome),
      status: issue_status(running, retry, recent_outcome),
      inspector_path: "/runs/#{issue_identifier}",
      event_count: length(recent_events),
      workspace: %{
        path: workspace_path(issue_identifier, running, retry, recent_outcome),
        host: workspace_host(running, retry, recent_outcome)
      },
      attempts: %{
        restart_count: restart_count(retry),
        current_retry_attempt: retry_attempt(retry)
      },
      running: running && running_issue_payload(running),
      retry: retry && retry_issue_payload(retry),
      recent_outcome: recent_outcome && completed_issue_payload(recent_outcome),
      logs: %{
        codex_session_logs: recent_events
      },
      recent_events: recent_events,
      operator_transcript: operator_transcript_payload(recent_events),
      last_error: retry && retry.error,
      tracked: %{}
    }
  end

  defp sort_running_entries(entries) when is_list(entries) do
    Enum.sort_by(entries, &issue_identifier_sort_key/1)
  end

  defp sort_running_entries(_entries), do: []

  defp issue_identifier_sort_key(%{identifier: identifier}) when is_binary(identifier) do
    identifier = String.trim(identifier)

    case Regex.run(~r/^([[:alpha:]]+)-(\d+)$/, identifier) do
      [_, prefix, number] -> {0, String.downcase(prefix), String.to_integer(number), identifier}
      _ -> {1, String.downcase(identifier), identifier}
    end
  end

  defp issue_identifier_sort_key(entry) when is_map(entry) do
    fallback = Map.get(entry, :issue_id) || ""
    {2, to_string(fallback)}
  end

  defp issue_identifier_sort_key(_entry), do: {3, ""}

  defp merge_recent_outcomes(live_recent_outcomes, archived_recent_outcomes) do
    (live_recent_outcomes ++ archived_recent_outcomes)
    |> Enum.reduce({MapSet.new(), []}, fn outcome, {seen, acc} ->
      identifier = Map.get(outcome, :identifier)

      if is_binary(identifier) and MapSet.member?(seen, identifier) do
        {seen, acc}
      else
        {MapSet.put(seen, identifier), acc ++ [outcome]}
      end
    end)
    |> elem(1)
  end

  defp alerts_payload(snapshot) when is_map(snapshot) do
    snapshot.running
    |> Enum.flat_map(&running_alerts_for_entry/1)
    |> Enum.sort_by(&alert_sort_key/1)
  end

  defp issue_id_from_entries(running, retry, recent_outcome),
    do: (running && running.issue_id) || (retry && retry.issue_id) || (recent_outcome && Map.get(recent_outcome, :issue_id))

  defp issue_title(running, retry, recent_outcome),
    do: (running && Map.get(running, :title)) || (retry && Map.get(retry, :title)) || (recent_outcome && Map.get(recent_outcome, :title))

  defp issue_url_from_entries(running, retry, recent_outcome),
    do: (running && (Map.get(running, :url) || get_in(running, [:issue, :url]))) || (retry && Map.get(retry, :url)) || (recent_outcome && Map.get(recent_outcome, :url))

  defp restart_count(retry), do: max(retry_attempt(retry) - 1, 0)
  defp retry_attempt(nil), do: 0
  defp retry_attempt(retry), do: retry.attempt || 0

  defp issue_status(_running, _retry, %{} = recent_outcome),
    do: normalize_outcome_status(Map.get(recent_outcome, :outcome))

  defp issue_status(_running, nil, _recent_outcome), do: "running"
  defp issue_status(nil, _retry, _recent_outcome), do: "retrying"
  defp issue_status(_running, _retry, _recent_outcome), do: "running"

  defp running_entry_payload(entry) do
    {health, health_reason} = health_for_running_entry(entry)

    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      issue_url: Map.get(entry, :url) || get_in(entry, [:issue, :url]),
      title: Map.get(entry, :title),
      state: entry.state,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: entry.session_id,
      thread_id: Map.get(entry, :thread_id),
      current_turn_id: Map.get(entry, :current_turn_id),
      turn_count: Map.get(entry, :turn_count, 0),
      last_event: entry.last_codex_event,
      last_message: summarize_message(entry.last_codex_message),
      progress_phase: Map.get(entry, :progress_phase),
      progress_detail: Map.get(entry, :progress_detail),
      progress_updated_at: iso8601(Map.get(entry, :progress_updated_at)),
      started_at: iso8601(entry.started_at),
      last_event_at: iso8601(entry.last_codex_timestamp),
      last_activity_seconds: last_activity_seconds(entry),
      event_count: length(Map.get(entry, :recent_codex_events, [])),
      burn_rate_tokens_per_min: burn_rate_tokens_per_min(entry),
      health: health,
      health_reason: health_reason,
      tokens: %{
        input_tokens: entry.codex_input_tokens,
        output_tokens: entry.codex_output_tokens,
        total_tokens: entry.codex_total_tokens
      }
    }
  end

  defp retry_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      issue_url: Map.get(entry, :url),
      attempt: entry.attempt,
      due_at: due_at_iso8601(entry.due_in_ms),
      error: entry.error,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path)
    }
  end

  defp running_issue_payload(running) do
    {health, health_reason} = health_for_running_entry(running)

    %{
      worker_host: Map.get(running, :worker_host),
      workspace_path: Map.get(running, :workspace_path),
      issue_url: Map.get(running, :url) || get_in(running, [:issue, :url]),
      session_id: running.session_id,
      thread_id: Map.get(running, :thread_id),
      current_turn_id: Map.get(running, :current_turn_id),
      turn_count: Map.get(running, :turn_count, 0),
      state: running.state,
      started_at: iso8601(running.started_at),
      last_event: running.last_codex_event,
      last_message: summarize_message(running.last_codex_message),
      progress_phase: Map.get(running, :progress_phase),
      progress_detail: Map.get(running, :progress_detail),
      progress_updated_at: iso8601(Map.get(running, :progress_updated_at)),
      last_event_at: iso8601(running.last_codex_timestamp),
      last_activity_seconds: last_activity_seconds(running),
      event_count: length(Map.get(running, :recent_codex_events, [])),
      burn_rate_tokens_per_min: burn_rate_tokens_per_min(running),
      health: health,
      health_reason: health_reason,
      tokens: %{
        input_tokens: running.codex_input_tokens,
        output_tokens: running.codex_output_tokens,
        total_tokens: running.codex_total_tokens
      }
    }
  end

  defp retry_issue_payload(retry) do
    %{
      attempt: retry.attempt,
      due_at: due_at_iso8601(retry.due_in_ms),
      error: retry.error,
      issue_url: Map.get(retry, :url),
      worker_host: Map.get(retry, :worker_host),
      workspace_path: Map.get(retry, :workspace_path)
    }
  end

  defp completed_issue_payload(recent_outcome) do
    runtime_seconds = Map.get(recent_outcome, :runtime_seconds, 0)
    tokens = Map.get(recent_outcome, :tokens, %{})
    outcome = Map.get(recent_outcome, :outcome)
    status = normalize_outcome_status(outcome)

    %{
      worker_host: Map.get(recent_outcome, :worker_host),
      workspace_path: Map.get(recent_outcome, :workspace_path),
      issue_url: Map.get(recent_outcome, :url),
      session_id: Map.get(recent_outcome, :session_id),
      thread_id: Map.get(recent_outcome, :thread_id) || Map.get(recent_outcome, :session_id),
      current_turn_id: Map.get(recent_outcome, :current_turn_id),
      turn_count: Map.get(recent_outcome, :turn_count, 0),
      status: status,
      outcome: outcome,
      state: completed_state_label(status),
      started_at: iso8601(Map.get(recent_outcome, :started_at)),
      finished_at: iso8601(Map.get(recent_outcome, :finished_at)),
      runtime_seconds: runtime_seconds,
      last_event: Map.get(recent_outcome, :last_event),
      last_message: Map.get(recent_outcome, :last_message),
      progress_phase: completed_state_label(status),
      progress_detail: Map.get(recent_outcome, :last_message) || "Run finished",
      last_event_at: iso8601(Map.get(recent_outcome, :last_event_at) || Map.get(recent_outcome, :finished_at)),
      last_activity_seconds: nil,
      event_count: archived_event_count(recent_outcome),
      burn_rate_tokens_per_min: burn_rate_from_totals(tokens, runtime_seconds),
      health: completed_health(status),
      health_reason: completed_health_reason(status, outcome, recent_outcome),
      tokens: tokens
    }
  end

  defp recent_outcome_payload(outcome) do
    transcript =
      outcome
      |> archived_or_recent_events()
      |> operator_transcript_payload()

    display_message = recent_outcome_display_message(transcript, outcome)

    %{
      issue_id: Map.get(outcome, :issue_id),
      issue_identifier: Map.get(outcome, :identifier),
      issue_url: Map.get(outcome, :url),
      title: Map.get(outcome, :title),
      outcome: Map.get(outcome, :outcome),
      status: normalize_outcome_status(Map.get(outcome, :outcome)),
      state: completed_state_label(normalize_outcome_status(Map.get(outcome, :outcome))),
      session_id: Map.get(outcome, :session_id),
      last_event: Map.get(outcome, :last_event),
      last_message: Map.get(outcome, :last_message),
      runtime_seconds: Map.get(outcome, :runtime_seconds),
      finished_at: iso8601(Map.get(outcome, :finished_at)),
      tokens: Map.get(outcome, :tokens, %{}),
      display_message: display_message,
      display_message_preview: truncate_text(display_message, 140),
      display_message_expandable: expandable_text?(display_message, 140)
    }
  end

  defp archived_or_recent_events(outcome) do
    case Map.get(outcome, :archived_events, []) do
      [_ | _] = archived_events -> archived_events
      _ -> Enum.map(Map.get(outcome, :recent_codex_events, []), &event_payload/1)
    end
  end

  defp archived_event_count(entry) do
    case Map.get(entry, :archived_events, []) do
      [_ | _] = archived_events -> length(archived_events)
      _ -> length(Map.get(entry, :recent_codex_events, []))
    end
  end

  defp recent_outcome_display_message(transcript, outcome) when is_list(transcript) do
    assistant_block =
      transcript
      |> Enum.reverse()
      |> Enum.find(&(Map.get(&1, :kind) == "assistant" and not blank_text?(Map.get(&1, :body))))

    case assistant_block do
      %{body: body} when is_binary(body) and body != "" -> body
      _ -> Map.get(outcome, :last_message) || Map.get(outcome, :last_event)
    end
  end

  defp recent_outcome_display_message(_transcript, outcome),
    do: Map.get(outcome, :last_message) || Map.get(outcome, :last_event)

  defp truncate_text(text, limit) when is_binary(text) and is_integer(limit) and limit > 3 do
    normalized = String.trim(text)

    if String.length(normalized) > limit do
      String.slice(normalized, 0, limit - 3) <> "..."
    else
      normalized
    end
  end

  defp truncate_text(text, _limit), do: text

  defp expandable_text?(text, limit) when is_binary(text) and is_integer(limit),
    do: String.length(String.trim(text)) > limit

  defp expandable_text?(_text, _limit), do: false

  defp workspace_path(issue_identifier, running, retry, recent_outcome) do
    (running && Map.get(running, :workspace_path)) ||
      (retry && Map.get(retry, :workspace_path)) ||
      (recent_outcome && Map.get(recent_outcome, :workspace_path)) ||
      Path.join(Config.settings!().workspace.root, issue_identifier)
  end

  defp workspace_host(running, retry, recent_outcome) do
    (running && Map.get(running, :worker_host)) ||
      (retry && Map.get(retry, :worker_host)) ||
      (recent_outcome && Map.get(recent_outcome, :worker_host))
  end

  defp event_source(nil, recent_outcome), do: recent_outcome
  defp event_source(running, _recent_outcome), do: running

  defp recent_events_payload(nil), do: []

  defp recent_events_payload(entry) do
    entry
    |> raw_recent_events_payload()
    |> compact_recent_events_payload()
    |> sanitize_recent_events_payload()
  end

  defp raw_recent_events_payload(nil), do: []

  defp raw_recent_events_payload(entry) do
    case Map.get(entry, :archived_events, []) do
      [_ | _] = archived_events ->
        archived_events

      _ ->
        events = Map.get(entry, :recent_codex_events, [])

        case events do
          [] ->
            fallback_recent_events_payload(entry)

          _ ->
            Enum.map(events, &event_payload/1)
        end
    end
  end

  defp compact_recent_events_payload(events) when is_list(events) do
    events
    |> Enum.reduce([], &reduce_compact_recent_event/2)
    |> Enum.reverse()
  end

  defp compact_recent_events_payload(_events), do: []

  defp sanitize_recent_events_payload(events) when is_list(events) do
    Enum.map(events, fn event ->
      event
      |> Map.delete(:payload_events)
      |> Map.delete("payload_events")
    end)
  end

  defp sanitize_recent_events_payload(events), do: events

  defp reduce_compact_recent_event(event, [current | rest] = acc) do
    if mergeable_stream_event?(current, event) do
      [merge_stream_event(current, event) | rest]
    else
      [event | acc]
    end
  end

  defp reduce_compact_recent_event(event, acc), do: [event | acc]

  defp mergeable_stream_event?(current, event) do
    stream_event?(current) and
      stream_event?(event) and
      event_field(current, :method) == event_field(event, :method) and
      event_field(current, :item_id) == event_field(event, :item_id) and
      event_field(current, :turn_id) == event_field(event, :turn_id)
  end

  defp merge_stream_event(current, event) do
    merged_message =
      [extract_stream_message_text(event_field(current, :message)), stream_delta_text(event)]
      |> Enum.join("")

    payload_events = [Map.get(current, :payload_events, [current]), [event]] |> List.flatten()

    current
    |> put_event_field(:at, event_field(event, :at) || event_field(current, :at))
    |> put_event_field(:summary, stream_event_summary(current, merged_message))
    |> put_event_field(:message, merged_message)
    |> put_event_field(:raw, transcript_raw(payload_events))
    |> put_event_field(:payload_events, payload_events)
  end

  defp stream_event?(event) do
    event_field(event, :method) in [
      "item/agentMessage/delta",
      "codex/event/agent_message_delta",
      "codex/event/agent_message_content_delta",
      "item/reasoning/textDelta",
      "item/reasoning/summaryTextDelta",
      "item/reasoning/summaryPartAdded",
      "item/plan/delta",
      "codex/event/agent_reasoning_delta",
      "codex/event/reasoning_content_delta"
    ]
  end

  defp extract_stream_message_text(message) when is_binary(message) do
    String.replace_prefix(message, stream_message_prefix(message), "")
  end

  defp extract_stream_message_text(_message), do: ""

  defp stream_delta_text(event),
    do: extract_stream_delta(event) || extract_stream_message_text(event_field(event, :message))

  defp stream_message_prefix(message) do
    cond do
      String.starts_with?(message, "agent message streaming: ") -> "agent message streaming: "
      String.starts_with?(message, "reasoning streaming: ") -> "reasoning streaming: "
      true -> ""
    end
  end

  defp stream_event_summary(event, merged_message) do
    prefix =
      case event_field(event, :method) do
        method when method in ["item/agentMessage/delta", "codex/event/agent_message_delta", "codex/event/agent_message_content_delta"] ->
          "agent message streaming: "

        _ ->
          "reasoning streaming: "
      end

    prefix <> merged_message
  end

  defp fallback_recent_events_payload(entry) do
    summary = summarize_entry_message(entry)

    [
      %{
        at: iso8601(Map.get(entry, :last_codex_timestamp) || Map.get(entry, :last_event_at) || Map.get(entry, :finished_at)),
        event: Map.get(entry, :last_codex_event) || Map.get(entry, :last_event),
        method: nil,
        category: "system",
        summary: summary,
        message: summary,
        progress_phase: Map.get(entry, :progress_phase),
        progress_detail: Map.get(entry, :progress_detail),
        session_id: Map.get(entry, :session_id),
        thread_id: Map.get(entry, :thread_id),
        turn_id: Map.get(entry, :current_turn_id),
        item_id: nil,
        raw: nil,
        payload: nil
      }
    ]
    |> Enum.reject(&is_nil(&1.at))
  end

  defp event_payload(event) do
    %{
      event_id: Map.get(event, :event_id),
      at: iso8601(Map.get(event, :timestamp)),
      event: Map.get(event, :event),
      method: Map.get(event, :method),
      category: Map.get(event, :category),
      summary: Map.get(event, :summary),
      message: Map.get(event, :summary),
      session_id: Map.get(event, :session_id),
      thread_id: Map.get(event, :thread_id),
      turn_id: Map.get(event, :turn_id),
      item_id: Map.get(event, :item_id),
      raw: Map.get(event, :raw),
      payload: Map.get(event, :payload),
      payload_events: [event]
    }
  end

  defp event_field(event, field) when is_atom(field) and is_map(event) do
    Map.get(event, field) || Map.get(event, Atom.to_string(field))
  end

  defp event_field(_event, _field), do: nil

  defp put_event_field(event, field, value) when is_atom(field) and is_map(event) do
    event
    |> Map.put(field, value)
    |> Map.put(Atom.to_string(field), value)
  end

  defp put_event_field(event, _field, _value), do: event

  defp operator_transcript_payload(events) when is_list(events) do
    events
    |> Enum.reduce(%{blocks: [], buffers: %{assistant: %{}, thinking: %{}}}, &reduce_operator_transcript/2)
    |> flush_operator_transcript_buffers()
    |> Map.get(:blocks)
  end

  defp operator_transcript_payload(_events), do: []

  defp reduce_operator_transcript(event, state) when is_map(event) do
    cond do
      assistant_delta_event?(event) ->
        append_transcript_buffer(state, :assistant, "Assistant", event)

      thinking_delta_event?(event) ->
        append_transcript_buffer(state, :thinking, "Thinking", event)

      assistant_completed_event?(event) ->
        emit_completed_text_block(state, :assistant, "Assistant", event)

      thinking_completed_event?(event) ->
        emit_completed_text_block(state, :thinking, "Thinking", event)

      command_completed_event?(event) ->
        emit_command_blocks(state, event)

      tool_call_event?(event) ->
        emit_tool_blocks(state, event)

      file_change_completed_event?(event) ->
        emit_file_change_block(state, event)

      true ->
        state
    end
  end

  defp reduce_operator_transcript(_event, state), do: state

  defp append_transcript_buffer(state, buffer_kind, title, event) do
    delta = extract_stream_delta(event)
    item_id = transcript_item_id(event)

    if blank_text?(delta) or is_nil(item_id) do
      state
    else
      existing = get_in(state, [:buffers, buffer_kind, item_id]) || %{title: title, body: "", timestamp: event_timestamp(event), raw_events: []}

      updated = %{
        existing
        | body: Map.get(existing, :body, "") <> delta,
          timestamp: Map.get(existing, :timestamp) || event_timestamp(event),
          raw_events: Map.get(existing, :raw_events, []) ++ [event]
      }

      put_in(state, [:buffers, buffer_kind, item_id], updated)
    end
  end

  defp emit_completed_text_block(state, buffer_kind, title, event) do
    item = transcript_item(event)
    item_id = transcript_item_id(event) || map_get(item, "id")
    buffer = get_in(state, [:buffers, buffer_kind, item_id])

    text =
      case buffer_kind do
        :assistant -> assistant_text(item) || buffer_text(buffer)
        :thinking -> thinking_text(item) || buffer_text(buffer)
      end

    state = delete_transcript_buffer(state, buffer_kind, item_id)

    if blank_text?(text) do
      state
    else
      append_transcript_block(state, %{
        kind: Atom.to_string(buffer_kind),
        title: title,
        body: String.trim(text),
        timestamp: event_timestamp(event),
        raw: transcript_raw([(buffer && Map.get(buffer, :raw_events, [])) || [], [event]] |> List.flatten())
      })
    end
  end

  defp emit_command_blocks(state, event) do
    item = transcript_item(event)
    command = map_get(item, "command")
    output = map_get(item, "aggregatedOutput")
    exit_code = map_get(item, "exitCode")
    timestamp = event_timestamp(event)
    raw = transcript_raw([event])

    state =
      if blank_text?(command) do
        state
      else
        append_transcript_block(state, %{
          kind: "command",
          title: "Command",
          body: String.trim(command),
          timestamp: timestamp,
          raw: raw,
          meta: command_meta(exit_code)
        })
      end

    if blank_text?(output) do
      state
    else
      append_transcript_block(state, %{
        kind: "command_output",
        title: "Command output",
        body: String.trim_trailing(output),
        timestamp: timestamp,
        raw: raw
      })
    end
  end

  defp emit_tool_blocks(state, event) do
    payload = Map.get(event, :payload) || Map.get(event, "payload") || %{}
    tool = dynamic_tool_name(payload)
    arguments = map_get_in(payload, ["params", "arguments"]) || %{}
    timestamp = event_timestamp(event)
    raw = transcript_raw([event])
    title_suffix = if blank_text?(tool), do: "tool", else: tool

    state =
      append_transcript_block(state, %{
        kind: "tool_call",
        title: "Tool call #{title_suffix}",
        body: pretty_jsonish(arguments),
        timestamp: timestamp,
        raw: raw
      })

    append_transcript_block(state, %{
      kind: "tool_response",
      title: "Tool response #{title_suffix}",
      body: tool_response_summary(event),
      timestamp: timestamp,
      raw: raw
    })
  end

  defp emit_file_change_block(state, event) do
    item = transcript_item(event)
    changes = map_get(item, "changes") || []
    body = format_file_changes(changes)

    if blank_text?(body) do
      state
    else
      append_transcript_block(state, %{
        kind: "file_change",
        title: "File change",
        body: body,
        timestamp: event_timestamp(event),
        raw: transcript_raw([event])
      })
    end
  end

  defp flush_operator_transcript_buffers(%{buffers: buffers} = state) when is_map(buffers) do
    leftovers =
      [:thinking, :assistant]
      |> Enum.flat_map(fn buffer_kind ->
        buffers
        |> Map.get(buffer_kind, %{})
        |> Enum.map(fn {_item_id, buffer} ->
          %{
            kind: Atom.to_string(buffer_kind),
            title: Map.get(buffer, :title),
            body: String.trim(Map.get(buffer, :body, "")),
            timestamp: Map.get(buffer, :timestamp),
            raw: transcript_raw(Map.get(buffer, :raw_events, []))
          }
        end)
      end)
      |> Enum.reject(&blank_text?(&1.body))
      |> Enum.sort_by(&(&1.timestamp || ""))

    %{state | blocks: Map.get(state, :blocks, []) ++ leftovers, buffers: %{assistant: %{}, thinking: %{}}}
  end

  defp flush_operator_transcript_buffers(state), do: state

  defp append_transcript_block(state, block) do
    %{state | blocks: Map.get(state, :blocks, []) ++ [Map.drop(block, [:meta]) |> maybe_put_meta(block)]}
  end

  defp maybe_put_meta(block, %{meta: meta}) when is_binary(meta) and meta != "", do: Map.put(block, :meta, meta)
  defp maybe_put_meta(block, _full_block), do: block

  defp delete_transcript_buffer(state, _buffer_kind, nil), do: state

  defp delete_transcript_buffer(state, buffer_kind, item_id) do
    update_in(state, [:buffers, buffer_kind], fn buffer_map ->
      (buffer_map || %{})
      |> Map.delete(item_id)
    end)
  end

  defp assistant_completed_event?(event), do: completed_item_type(event) == "agentMessage"
  defp thinking_completed_event?(event), do: completed_item_type(event) == "reasoning"
  defp command_completed_event?(event), do: completed_item_type(event) == "commandExecution"
  defp file_change_completed_event?(event), do: completed_item_type(event) == "fileChange"

  defp completed_item_type(event) do
    if event_method(event) == "item/completed" do
      event
      |> transcript_item()
      |> map_get("type")
    end
  end

  defp tool_call_event?(event), do: event_method(event) == "item/tool/call"

  defp assistant_delta_event?(event) do
    event_method(event) in [
      "item/agentMessage/delta",
      "codex/event/agent_message_delta",
      "codex/event/agent_message_content_delta"
    ]
  end

  defp thinking_delta_event?(event) do
    event_method(event) in [
      "item/reasoning/textDelta",
      "item/reasoning/summaryTextDelta",
      "item/reasoning/summaryPartAdded",
      "item/plan/delta",
      "codex/event/agent_reasoning_delta",
      "codex/event/reasoning_content_delta"
    ]
  end

  defp event_method(event), do: Map.get(event, :method) || Map.get(event, "method")
  defp event_timestamp(event), do: Map.get(event, :at) || Map.get(event, "at")

  defp transcript_item(event) do
    event
    |> Map.get(:payload)
    |> case do
      nil -> Map.get(event, "payload", %{})
      payload -> payload
    end
    |> map_get_in(["params", "item"])
  end

  defp transcript_item_id(event) do
    Map.get(event, :item_id) || Map.get(event, "item_id") || map_get(transcript_item(event), "id")
  end

  defp assistant_text(item), do: map_get(item, "text")

  defp thinking_text(item) do
    item
    |> collect_text_fragments()
    |> Enum.join("\n")
  end

  defp buffer_text(nil), do: nil
  defp buffer_text(buffer), do: Map.get(buffer, :body)

  defp extract_stream_delta(event) do
    payload = Map.get(event, :payload) || Map.get(event, "payload") || %{}

    map_get_in(payload, ["params", "delta"]) ||
      map_get_in(payload, ["params", "text"]) ||
      map_get_in(payload, ["params", "summaryText"]) ||
      map_get_in(payload, ["params", "part", "text"]) ||
      map_get_in(payload, ["params", "msg", "content"]) ||
      map_get_in(payload, ["params", "msg", "text"])
  end

  defp tool_response_summary(event) do
    summary = Map.get(event, :summary) || Map.get(event, "summary") || "tool call completed"

    cond do
      String.contains?(summary, "failed") -> "failed"
      String.contains?(summary, "completed") -> "completed"
      true -> summary
    end
  end

  defp dynamic_tool_name(payload) when is_map(payload) do
    map_get_in(payload, ["params", "tool"]) || map_get_in(payload, ["params", "name"])
  end

  defp dynamic_tool_name(_payload), do: nil

  defp format_file_changes(changes) when is_list(changes) do
    changes
    |> Enum.map(&format_file_change/1)
    |> Enum.reject(&blank_text?/1)
    |> Enum.join("\n")
  end

  defp format_file_changes(_changes), do: nil

  defp format_file_change(change) when is_map(change) do
    kind =
      change
      |> map_get("kind")
      |> map_get("type")
      |> case do
        "add" -> "A"
        "delete" -> "D"
        "remove" -> "D"
        "update" -> "M"
        "modify" -> "M"
        value when is_binary(value) and value != "" -> String.upcase(String.slice(value, 0, 1))
        _ -> "M"
      end

    path = map_get(change, "path")

    if blank_text?(path) do
      nil
    else
      "#{kind} #{Path.basename(path)}"
    end
  end

  defp command_meta(nil), do: nil
  defp command_meta(exit_code), do: "exit #{exit_code}"

  defp transcript_raw(events) when is_list(events) do
    events
    |> Enum.map(fn event ->
      %{
        at: event_timestamp(event),
        method: event_method(event),
        summary: Map.get(event, :summary) || Map.get(event, "summary"),
        payload: Map.get(event, :payload) || Map.get(event, "payload"),
        raw: Map.get(event, :raw) || Map.get(event, "raw")
      }
    end)
    |> inspect(pretty: true, limit: :infinity, printable_limit: :infinity)
  end

  defp transcript_raw(_events), do: nil

  defp pretty_jsonish(value) do
    try do
      Jason.encode!(json_safe(value), pretty: true)
    rescue
      _ -> inspect(value, pretty: true, limit: :infinity, printable_limit: :infinity)
    end
  end

  defp json_safe(%{} = map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), json_safe(value)} end)
    |> Enum.into(%{})
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value), do: value

  defp collect_text_fragments(nil), do: []
  defp collect_text_fragments(text) when is_binary(text), do: [text]

  defp collect_text_fragments(list) when is_list(list) do
    Enum.flat_map(list, &collect_text_fragments/1)
  end

  defp collect_text_fragments(%{} = map) do
    [
      map_get(map, "text"),
      map_get(map, "summaryText"),
      map_get(map, "content"),
      map_get(map, "summary")
    ]
    |> Enum.flat_map(&collect_text_fragments/1)
  end

  defp collect_text_fragments(_value), do: []

  defp blank_text?(value), do: value in [nil, ""] or (is_binary(value) and String.trim(value) == "")

  defp map_get(nil, _key), do: nil

  defp map_get(map, key) when is_map(map) and is_binary(key) do
    Map.get(map, key) || Map.get(map, safe_existing_atom(key))
  end

  defp map_get(map, key) when is_map(map), do: Map.get(map, key)

  defp safe_existing_atom(key) when is_binary(key) do
    try do
      String.to_existing_atom(key)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_key), do: nil

  defp map_get_in(map, []), do: map
  defp map_get_in(nil, _path), do: nil

  defp map_get_in(map, [key | rest]) when is_map(map) do
    map
    |> map_get(key)
    |> map_get_in(rest)
  end

  defp map_get_in(_map, _path), do: nil

  defp filter_events(events, params) when is_list(events) and is_map(params) do
    events
    |> filter_events_before(Map.get(params, "before"))
    |> filter_events_after(Map.get(params, "after"))
    |> filter_events_by_categories(Map.get(params, "categories"))
    |> filter_events_by_types(Map.get(params, "event_types"))
    |> maybe_filter_errors_only(Map.get(params, "errors_only"))
  end

  defp filter_events(events, _params) when is_list(events), do: events

  defp filter_events_before(events, nil), do: events

  defp filter_events_before(events, before) when is_list(events) do
    case parse_limit(before, nil) do
      nil -> events
      cursor -> Enum.filter(events, &event_id_before?(&1, cursor))
    end
  end

  defp filter_events_after(events, nil), do: events

  defp filter_events_after(events, after_cursor) when is_list(events) do
    case parse_limit(after_cursor, nil) do
      nil -> events
      cursor -> Enum.filter(events, &event_id_after?(&1, cursor))
    end
  end

  defp filter_events_by_categories(events, nil), do: events

  defp filter_events_by_categories(events, categories) when is_list(events) do
    wanted = split_filter_values(categories)

    if wanted == [] do
      events
    else
      Enum.filter(events, &(Map.get(&1, :category) in wanted or Map.get(&1, "category") in wanted))
    end
  end

  defp filter_events_by_types(events, nil), do: events

  defp filter_events_by_types(events, event_types) when is_list(events) do
    wanted = split_filter_values(event_types)

    if wanted == [] do
      events
    else
      Enum.filter(events, fn event ->
        method = Map.get(event, :method) || Map.get(event, "method")
        event_name = (Map.get(event, :event) || Map.get(event, "event") || "") |> to_string()
        method in wanted or event_name in wanted
      end)
    end
  end

  defp maybe_filter_errors_only(events, errors_only) when is_list(events) do
    if truthy_param?(errors_only) do
      Enum.filter(events, &error_event?/1)
    else
      events
    end
  end

  defp maybe_filter_errors_only(events, _errors_only), do: events

  defp split_filter_values(value) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp split_filter_values(values) when is_list(values), do: Enum.flat_map(values, &split_filter_values/1)
  defp split_filter_values(_value), do: []

  defp truthy_param?(value) when value in [true, "true", "1", 1], do: true
  defp truthy_param?(_value), do: false

  defp error_event?(event) do
    category = Map.get(event, :category) || Map.get(event, "category")
    method = Map.get(event, :method) || Map.get(event, "method") || ""
    event_name = (Map.get(event, :event) || Map.get(event, "event") || "") |> to_string()
    summary = Map.get(event, :summary) || Map.get(event, "summary") || ""

    category == "approval" or
      event_name in ["turn_failed", "turn_ended_with_error", "startup_failed"] or
      String.contains?(method, "failed") or String.contains?(summary, "failed") or String.contains?(summary, "error")
  end

  defp event_id_before?(event, cursor) when is_integer(cursor) do
    event_id = Map.get(event, :event_id) || Map.get(event, "event_id")
    is_integer(event_id) and event_id < cursor
  end

  defp event_id_after?(event, cursor) when is_integer(cursor) do
    event_id = Map.get(event, :event_id) || Map.get(event, "event_id")
    is_integer(event_id) and event_id > cursor
  end

  defp maybe_limit_events(events, params) when is_list(events) and is_map(params) do
    limit = params |> Map.get("limit") |> parse_limit(length(events))
    Enum.take(events, -limit)
  end

  defp parse_limit(nil, default), do: default

  defp parse_limit(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, _rest} when parsed > 0 -> parsed
      _ -> default
    end
  end

  defp parse_limit(value, _default) when is_integer(value) and value > 0, do: value
  defp parse_limit(_value, default), do: default

  defp running_alerts_for_entry(entry) do
    case health_for_running_entry(entry) do
      {"stalled", reason} ->
        [alert_payload("critical", entry.identifier, "stalled", reason)]

      {"waiting_approval", reason} ->
        [alert_payload("warning", entry.identifier, "waiting_approval", reason)]

      {"waiting_input", reason} ->
        [alert_payload("warning", entry.identifier, "waiting_input", reason)]

      {"warning", reason} ->
        [alert_payload("warning", entry.identifier, "warning", reason)]

      _ ->
        []
    end
  end

  defp alert_payload(severity, issue_identifier, code, message) do
    %{
      severity: severity,
      issue_identifier: issue_identifier,
      code: code,
      message: message
    }
  end

  defp alert_sort_key(%{severity: "critical"}), do: 0
  defp alert_sort_key(%{severity: "warning"}), do: 1
  defp alert_sort_key(_alert), do: 2

  defp health_for_running_entry(entry) do
    timeout_seconds = max(div(Config.settings!().codex.stall_timeout_ms, 1_000), 1)
    idle_seconds = last_activity_seconds(entry)
    event = Map.get(entry, :last_codex_event)

    cond do
      event == :approval_required ->
        {"waiting_approval", "Waiting on approval"}

      event == :turn_input_required ->
        {"waiting_input", "Waiting on non-interactive input"}

      idle_seconds >= timeout_seconds ->
        {"stalled", "No Codex activity for #{idle_seconds}s"}

      idle_seconds >= max(div(timeout_seconds, 2), 1) ->
        {"warning", "No Codex activity for #{idle_seconds}s"}

      true ->
        {"healthy", "Receiving Codex activity"}
    end
  end

  defp last_activity_seconds(entry) when is_map(entry) do
    timestamp = Map.get(entry, :last_codex_timestamp) || Map.get(entry, :last_event_at) || Map.get(entry, :finished_at) || Map.get(entry, :started_at)

    case timestamp do
      %DateTime{} = datetime ->
        max(DateTime.diff(DateTime.utc_now(), datetime, :second), 0)

      iso when is_binary(iso) ->
        case DateTime.from_iso8601(iso) do
          {:ok, parsed, _offset} -> max(DateTime.diff(DateTime.utc_now(), parsed, :second), 0)
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp burn_rate_tokens_per_min(entry) when is_map(entry) do
    runtime_seconds = runtime_seconds_from_started_at(Map.get(entry, :started_at), DateTime.utc_now())
    total_tokens = Map.get(entry, :codex_total_tokens, 0)

    cond do
      runtime_seconds <= 0 -> 0.0
      not is_integer(total_tokens) -> 0.0
      true -> Float.round(total_tokens / max(runtime_seconds / 60, 1.0e-6), 2)
    end
  end

  defp burn_rate_from_totals(tokens, runtime_seconds) when is_map(tokens) and is_number(runtime_seconds) do
    total_tokens = Map.get(tokens, :total_tokens) || Map.get(tokens, "total_tokens") || 0

    cond do
      runtime_seconds <= 0 -> 0.0
      not is_number(total_tokens) -> 0.0
      true -> Float.round(total_tokens / max(runtime_seconds / 60, 1.0e-6), 2)
    end
  end

  defp burn_rate_from_totals(_tokens, _runtime_seconds), do: 0.0

  defp summarize_message(nil), do: nil
  defp summarize_message(message), do: StatusDashboard.humanize_codex_message(message)

  defp summarize_entry_message(entry) when is_map(entry) do
    summarize_message(Map.get(entry, :last_codex_message) || Map.get(entry, :last_message))
  end

  defp summarize_entry_message(_entry), do: nil

  defp normalize_outcome_status(outcome) do
    case to_string(outcome) do
      value when value in ["completed_turn", "terminated", "completed"] -> "completed"
      value when value in ["cancelled", "canceled"] -> "cancelled"
      value when value != "" -> "failed"
      _ -> "completed"
    end
  end

  defp completed_state_label(status) do
    case to_string(status) do
      "completed" -> "Completed"
      "cancelled" -> "Cancelled"
      "failed" -> "Failed"
      value when value != "" -> Phoenix.Naming.humanize(value)
      _ -> "Completed"
    end
  end

  defp completed_health(status) do
    case to_string(status) do
      "completed" -> "healthy"
      "cancelled" -> "warning"
      _ -> "failed"
    end
  end

  defp completed_health_reason(status, outcome, recent_outcome) do
    message = Map.get(recent_outcome, :last_message)
    outcome_detail = outcome_detail_label(outcome, status)

    case {to_string(status), message} do
      {"completed", msg} when is_binary(msg) and msg != "" -> "Completed successfully · #{msg}"
      {"completed", _msg} -> "Completed successfully"
      {"cancelled", msg} when is_binary(msg) and msg != "" -> "Cancelled · #{msg}"
      {"cancelled", _msg} -> "Cancelled"
      {"failed", msg} when is_binary(msg) and msg != "" -> "Failed#{outcome_detail} · #{msg}"
      {"failed", _msg} -> "Failed#{outcome_detail}"
      _ -> "Run finished"
    end
  end

  defp outcome_detail_label(outcome, status) do
    case {to_string(outcome), to_string(status)} do
      {"", _} -> ""
      {raw, "failed"} -> " (#{raw})"
      _ -> ""
    end
  end

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

  defp iso8601(datetime) when is_binary(datetime) do
    case DateTime.from_iso8601(datetime) do
      {:ok, parsed, _offset} ->
        parsed
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()

      _ ->
        datetime
    end
  end

  defp iso8601(_datetime), do: nil

  defp runtime_seconds_from_started_at(%DateTime{} = started_at, %DateTime{} = now) do
    DateTime.diff(now, started_at, :second)
  end

  defp runtime_seconds_from_started_at(started_at, %DateTime{} = now) when is_binary(started_at) do
    case DateTime.from_iso8601(started_at) do
      {:ok, parsed, _offset} -> runtime_seconds_from_started_at(parsed, now)
      _ -> 0
    end
  end

  defp runtime_seconds_from_started_at(_started_at, _now), do: 0
end
