defmodule SymphonyElixir.Linear.Adapter do
  @moduledoc """
  Linear-backed tracker adapter.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.{AgentTool, Client}
  alias SymphonyElixir.Tracker.Issue

  @scope_pattern ~r/\A[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?\z/

  @type intake_scope :: {:project_slug, String.t()} | {:team_key, String.t()}

  @spec validate_config(map()) :: :ok | {:error, term()}
  def validate_config(tracker_settings) do
    cond do
      not present_string?(tracker_settings.endpoint) ->
        {:error, :invalid_linear_endpoint}

      not present_string?(tracker_settings.api_key) ->
        {:error, :missing_linear_api_token}

      not is_nil(tracker_settings.assignee) and not present_string?(tracker_settings.assignee) ->
        {:error, :invalid_linear_assignee}

      true ->
        case intake_scope(tracker_settings) do
          {:ok, _scope} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec intake_scope(map()) :: {:ok, intake_scope()} | {:error, term()}
  def intake_scope(tracker_settings) when is_map(tracker_settings) do
    project_slug = scope_value(Map.get(tracker_settings, :project_slug))
    team_key = scope_value(Map.get(tracker_settings, :team_key))

    case {project_slug, team_key} do
      {{:invalid, _value}, _} -> {:error, :invalid_linear_project_slug}
      {_, {:invalid, _value}} -> {:error, :invalid_linear_team_key}
      {{:present, _project_slug}, {:present, _team_key}} -> {:error, :conflicting_linear_intake_scope}
      {{:present, project_slug}, :missing} -> {:ok, {:project_slug, project_slug}}
      {:missing, {:present, team_key}} -> {:ok, {:team_key, team_key}}
      {:missing, :missing} -> {:error, :missing_linear_intake_scope}
    end
  end

  @spec fetch_issues_by_states([String.t()]) :: {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issues_by_states(states), do: client_module().fetch_issues_by_states(states)

  @spec fetch_issues_by_ids([String.t()]) :: {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issues_by_ids(issue_ids), do: client_module().fetch_issues_by_ids(issue_ids)

  @spec agent_tool_specs() :: [map()]
  def agent_tool_specs, do: AgentTool.tool_specs()

  @spec execute_agent_tool(String.t(), term(), keyword()) :: map()
  def execute_agent_tool(tool, arguments, opts) do
    AgentTool.execute(tool, arguments, opts)
  end

  @spec secret_environment_names(map()) :: [String.t()]
  def secret_environment_names(tracker_settings), do: tracker_settings.secret_environment_names

  defp client_module do
    Application.get_env(:symphony_elixir, :linear_client_module, Client)
  end

  defp scope_value(nil), do: :missing

  defp scope_value(value) when is_binary(value) do
    case String.trim(value) do
      "" ->
        :missing

      normalized ->
        if Regex.match?(@scope_pattern, normalized),
          do: {:present, normalized},
          else: {:invalid, value}
    end
  end

  defp scope_value(value), do: {:invalid, value}

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false
end
