defmodule SymphonyElixir.RunArchive do
  @moduledoc false

  require Logger

  alias SymphonyElixirWeb.Presenter

  @issues_dir "issues"
  @runs_dir "runs"

  @spec append_event(map(), map()) :: :ok
  def append_event(%{run_id: run_id, identifier: issue_identifier}, event)
      when is_binary(run_id) and is_binary(issue_identifier) and is_map(event) do
    with {:ok, root} <- fetch_root() do
      path = events_path(root, run_id)
      :ok = File.mkdir_p(Path.dirname(path))
      payload = Map.put(event, :issue_identifier, issue_identifier)
      File.write!(path, Jason.encode!(payload) <> "\n", [:append])
    end

    :ok
  end

  def append_event(_running_entry, _event), do: :ok

  @spec write_summary(map()) :: :ok
  def write_summary(%{run_id: run_id, identifier: issue_identifier} = summary)
      when is_binary(run_id) and is_binary(issue_identifier) do
    with {:ok, root} <- fetch_root() do
      run_dir = run_dir(root, run_id)
      :ok = File.mkdir_p(run_dir)

      serialized_summary = serialize_summary(summary, read_events(root, run_id))

      File.write!(summary_path(root, run_id), Jason.encode!(serialized_summary, pretty: true))
      :ok = File.mkdir_p(issues_dir(root))
      File.write!(issue_index_path(root, issue_identifier), Jason.encode!(serialize_issue_index(summary), pretty: true))
    end

    :ok
  end

  def write_summary(_summary), do: :ok

  @spec latest_issue(String.t()) :: {:ok, map()} | {:error, :not_found | :disabled}
  def latest_issue(issue_identifier) when is_binary(issue_identifier) do
    with {:ok, root} <- fetch_root(),
         {:ok, index} <- read_json(issue_index_path(root, issue_identifier)),
         run_id when is_binary(run_id) <- Map.get(index, "run_id"),
         {:ok, summary} <- read_json(summary_path(root, run_id)) do
      {:ok, normalize_summary(summary, index, read_events(root, run_id))}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :not_found}
    end
  end

  def latest_issue(_issue_identifier), do: {:error, :not_found}

  @spec recent_outcomes(non_neg_integer()) :: [map()]
  def recent_outcomes(limit \\ 50)

  def recent_outcomes(limit) when is_integer(limit) and limit > 0 do
    with {:ok, root} <- fetch_root(),
         true <- File.dir?(issues_dir(root)) do
      root
      |> issues_dir()
      |> Path.join("*.json")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        case read_json(path) do
          {:ok, index} ->
            [index]

          {:error, reason} ->
            Logger.debug("Failed to read archived issue index #{path}: #{inspect(reason)}")
            []
        end
      end)
      |> Enum.sort_by(&(Map.get(&1, "updated_at") || ""), :desc)
      |> Enum.take(limit)
      |> Enum.flat_map(fn index ->
        case Map.get(index, "issue_identifier") |> latest_issue() do
          {:ok, summary} -> [summary]
          {:error, _reason} -> []
        end
      end)
    else
      {:error, :disabled} -> []
      false -> []
      _ -> []
    end
  end

  def recent_outcomes(_limit), do: []

  defp fetch_root do
    case Application.get_env(:symphony_elixir, :run_archive_root) do
      root when is_binary(root) and root != "" -> {:ok, Path.expand(root)}
      _ -> {:error, :disabled}
    end
  end

  defp run_dir(root, run_id), do: Path.join([root, @runs_dir, safe_segment(run_id)])
  defp issues_dir(root), do: Path.join(root, @issues_dir)
  defp events_path(root, run_id), do: Path.join(run_dir(root, run_id), "events.jsonl")
  defp summary_path(root, run_id), do: Path.join(run_dir(root, run_id), "summary.json")
  defp issue_index_path(root, issue_identifier), do: Path.join(issues_dir(root), "#{safe_segment(issue_identifier)}.json")

  defp safe_segment(segment) do
    String.replace(segment, ~r/[^a-zA-Z0-9._-]/, "_")
  end

  defp serialize_summary(summary, archived_events) do
    %{
      run_id: Map.get(summary, :run_id),
      issue_id: Map.get(summary, :issue_id),
      issue_identifier: Map.get(summary, :identifier),
      issue_url: Map.get(summary, :url),
      title: Map.get(summary, :title),
      outcome: Map.get(summary, :outcome),
      session_id: Map.get(summary, :session_id),
      thread_id: Map.get(summary, :thread_id),
      current_turn_id: Map.get(summary, :current_turn_id),
      worker_host: Map.get(summary, :worker_host),
      workspace_path: Map.get(summary, :workspace_path),
      turn_count: Map.get(summary, :turn_count, 0),
      started_at: iso8601(Map.get(summary, :started_at)),
      finished_at: iso8601(Map.get(summary, :finished_at)),
      last_event: normalize_scalar(Map.get(summary, :last_event)),
      last_message: Map.get(summary, :last_message),
      last_event_at: iso8601(Map.get(summary, :last_event_at) || Map.get(summary, :finished_at)),
      runtime_seconds: Map.get(summary, :runtime_seconds, 0),
      tokens: normalize_tokens(Map.get(summary, :tokens, %{})),
      operator_transcript: Presenter.operator_transcript(archived_events)
    }
  end

  defp serialize_issue_index(summary) do
    %{
      issue_identifier: Map.get(summary, :identifier),
      issue_id: Map.get(summary, :issue_id),
      issue_url: Map.get(summary, :url),
      title: Map.get(summary, :title),
      outcome: Map.get(summary, :outcome),
      run_id: Map.get(summary, :run_id),
      latest_session_id: Map.get(summary, :session_id),
      updated_at: iso8601(Map.get(summary, :finished_at))
    }
  end

  defp normalize_summary(summary, index, archived_events) do
    %{
      run_id: Map.get(summary, "run_id") || Map.get(index, "run_id"),
      issue_id: Map.get(summary, "issue_id") || Map.get(index, "issue_id"),
      identifier: Map.get(summary, "issue_identifier") || Map.get(index, "issue_identifier"),
      url: Map.get(summary, "issue_url") || Map.get(index, "issue_url"),
      title: Map.get(summary, "title") || Map.get(index, "title"),
      outcome: Map.get(summary, "outcome") || Map.get(index, "outcome") || "completed",
      session_id: Map.get(summary, "session_id") || Map.get(index, "latest_session_id"),
      thread_id: Map.get(summary, "thread_id") || Map.get(summary, "session_id") || Map.get(index, "latest_session_id"),
      current_turn_id: Map.get(summary, "current_turn_id"),
      worker_host: Map.get(summary, "worker_host"),
      workspace_path: Map.get(summary, "workspace_path"),
      turn_count: Map.get(summary, "turn_count") || 0,
      started_at: Map.get(summary, "started_at"),
      finished_at: Map.get(summary, "finished_at") || Map.get(index, "updated_at"),
      last_event: Map.get(summary, "last_event"),
      last_message: Map.get(summary, "last_message"),
      last_event_at: Map.get(summary, "last_event_at") || Map.get(summary, "finished_at") || Map.get(index, "updated_at"),
      runtime_seconds: Map.get(summary, "runtime_seconds") || 0,
      tokens: normalize_tokens(Map.get(summary, "tokens", %{})),
      archived_events: archived_events,
      operator_transcript: Map.get(summary, "operator_transcript") || []
    }
  end

  defp read_json(path) do
    case File.read(path) do
      {:ok, contents} ->
        case Jason.decode(contents) do
          {:ok, payload} when is_map(payload) -> {:ok, payload}
          {:ok, _payload} -> {:error, :invalid_json_payload}
          {:error, reason} -> {:error, reason}
        end

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_events(root, run_id) do
    path = events_path(root, run_id)

    case File.read(path) do
      {:ok, contents} ->
        contents
        |> String.split("\n", trim: true)
        |> Enum.flat_map(fn line ->
          case Jason.decode(line) do
            {:ok, event} when is_map(event) ->
              [event]

            {:ok, _event} ->
              []

            {:error, reason} ->
              Logger.debug("Failed to decode archived event #{path}: #{inspect(reason)}")
              []
          end
        end)

      {:error, :enoent} ->
        []

      {:error, reason} ->
        Logger.debug("Failed to read archived events #{path}: #{inspect(reason)}")
        []
    end
  end

  defp normalize_tokens(tokens) when is_map(tokens) do
    %{
      input_tokens: integer_or_zero(Map.get(tokens, :input_tokens) || Map.get(tokens, "input_tokens")),
      output_tokens: integer_or_zero(Map.get(tokens, :output_tokens) || Map.get(tokens, "output_tokens")),
      total_tokens: integer_or_zero(Map.get(tokens, :total_tokens) || Map.get(tokens, "total_tokens"))
    }
  end

  defp normalize_tokens(_tokens), do: %{input_tokens: 0, output_tokens: 0, total_tokens: 0}

  defp integer_or_zero(value) when is_integer(value), do: value
  defp integer_or_zero(_value), do: 0

  defp normalize_scalar(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_scalar(value), do: value

  defp iso8601(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp iso8601(value) when is_binary(value), do: value
  defp iso8601(_value), do: nil
end
