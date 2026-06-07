defmodule SymphonyElixir.Config do
  @moduledoc """
  Runtime configuration loaded from `WORKFLOW.md`.
  """

  alias SymphonyElixir.{Config.Schema, PathSafety}
  alias SymphonyElixir.Workflow

  @default_prompt_template """
  You are working on a Linear issue.

  Identifier: {{ issue.identifier }}
  Title: {{ issue.title }}

  Body:
  {% if issue.description %}
  {{ issue.description }}
  {% else %}
  No description provided.
  {% endif %}
  """

  @type codex_runtime_settings :: %{
          approval_policy: String.t() | map(),
          thread_sandbox: String.t(),
          turn_sandbox_policy: map()
        }

  @spec settings() :: {:ok, Schema.t()} | {:error, term()}
  def settings do
    case Workflow.current() do
      {:ok, %{config: config}} when is_map(config) ->
        Schema.parse(config)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec settings!() :: Schema.t()
  def settings! do
    case settings() do
      {:ok, settings} ->
        settings

      {:error, reason} ->
        raise ArgumentError, message: format_config_error(reason)
    end
  end

  @spec max_concurrent_agents_for_state(term()) :: pos_integer()
  def max_concurrent_agents_for_state(state_name) when is_binary(state_name) do
    config = settings!()

    Map.get(
      config.agent.max_concurrent_agents_by_state,
      Schema.normalize_issue_state(state_name),
      config.agent.max_concurrent_agents
    )
  end

  def max_concurrent_agents_for_state(_state_name), do: settings!().agent.max_concurrent_agents

  @spec codex_turn_sandbox_policy(Path.t() | nil) :: map()
  def codex_turn_sandbox_policy(workspace \\ nil) do
    case runtime_turn_sandbox_policy(settings!(), workspace) do
      {:ok, policy} ->
        policy

      {:error, reason} ->
        raise ArgumentError, message: "Invalid codex turn sandbox policy: #{inspect(reason)}"
    end
  end

  @spec workflow_prompt() :: String.t()
  def workflow_prompt do
    case Workflow.current() do
      {:ok, %{prompt_template: prompt}} ->
        if String.trim(prompt) == "", do: @default_prompt_template, else: prompt

      _ ->
        @default_prompt_template
    end
  end

  @spec server_port() :: non_neg_integer() | nil
  def server_port do
    case Application.get_env(:symphony_elixir, :server_port_override) do
      port when is_integer(port) and port >= 0 -> port
      _ -> settings!().server.port
    end
  end

  @spec validate!() :: :ok | {:error, term()}
  def validate! do
    with {:ok, settings} <- settings() do
      validate_semantics(settings)
    end
  end

  @spec codex_runtime_settings(Path.t() | nil, keyword()) ::
          {:ok, codex_runtime_settings()} | {:error, term()}
  def codex_runtime_settings(workspace \\ nil, opts \\ []) do
    with {:ok, settings} <- settings() do
      with {:ok, turn_sandbox_policy} <- runtime_turn_sandbox_policy(settings, workspace, opts) do
        {:ok,
         %{
           approval_policy: settings.codex.approval_policy,
           thread_sandbox: settings.codex.thread_sandbox,
           turn_sandbox_policy: turn_sandbox_policy
         }}
      end
    end
  end

  defp runtime_turn_sandbox_policy(settings, workspace, opts \\ []) do
    with {:ok, policy} <- Schema.resolve_runtime_turn_sandbox_policy(settings, workspace, opts) do
      add_local_workspace_write_roots(policy, workspace, opts)
    end
  end

  defp add_local_workspace_write_roots(%{"type" => "workspaceWrite"} = policy, workspace, opts)
       when is_binary(workspace) and workspace != "" do
    if Keyword.get(opts, :remote, false) do
      {:ok, put_writable_roots(policy, [workspace])}
    else
      with {:ok, canonical_workspace} <- PathSafety.canonicalize(Path.expand(workspace)) do
        roots = [canonical_workspace] ++ git_metadata_roots(canonical_workspace)
        {:ok, put_writable_roots(policy, roots)}
      end
    end
  end

  defp add_local_workspace_write_roots(policy, _workspace, _opts), do: {:ok, policy}

  defp put_writable_roots(policy, roots) do
    existing_roots =
      case Map.get(policy, "writableRoots") do
        roots when is_list(roots) -> roots
        _ -> []
      end

    Map.put(policy, "writableRoots", Enum.uniq(existing_roots ++ roots))
  end

  defp git_metadata_roots(workspace) do
    workspace
    |> git_metadata_root_candidates()
    |> Enum.flat_map(&canonical_git_root/1)
    |> Enum.uniq()
  end

  defp git_metadata_root_candidates(workspace) do
    [
      git_rev_parse_path(workspace, "--absolute-git-dir"),
      git_rev_parse_path(workspace, "--git-common-dir")
    ]
  end

  defp git_rev_parse_path(workspace, arg) do
    case System.cmd("git", ["-C", workspace, "rev-parse", arg], stderr_to_stdout: true) do
      {raw_path, 0} ->
        raw_path
        |> String.trim()
        |> expand_git_path(workspace)

      _ ->
        ""
    end
  rescue
    _ -> ""
  end

  defp expand_git_path("", _workspace), do: ""

  defp expand_git_path(path, workspace) do
    case Path.type(path) do
      :absolute -> path
      _ -> Path.expand(path, workspace)
    end
  end

  defp canonical_git_root(""), do: []

  defp canonical_git_root(path) do
    case PathSafety.canonicalize(path) do
      {:ok, canonical_path} -> [canonical_path]
      _ -> []
    end
  end

  defp validate_semantics(settings) do
    cond do
      is_nil(settings.tracker.kind) ->
        {:error, :missing_tracker_kind}

      settings.tracker.kind not in ["linear", "memory"] ->
        {:error, {:unsupported_tracker_kind, settings.tracker.kind}}

      settings.tracker.kind == "linear" and not is_binary(settings.tracker.api_key) ->
        {:error, :missing_linear_api_token}

      true ->
        :ok
    end
  end

  defp format_config_error(reason) do
    case reason do
      {:invalid_workflow_config, message} ->
        "Invalid WORKFLOW.md config: #{message}"

      {:missing_workflow_file, path, raw_reason} ->
        "Missing WORKFLOW.md at #{path}: #{inspect(raw_reason)}"

      {:workflow_parse_error, raw_reason} ->
        "Failed to parse WORKFLOW.md: #{inspect(raw_reason)}"

      :workflow_front_matter_not_a_map ->
        "Failed to parse WORKFLOW.md: workflow front matter must decode to a map"

      other ->
        "Invalid WORKFLOW.md config: #{inspect(other)}"
    end
  end
end
