defmodule SymphonyElixir.LiveCodexObservabilityTest do
  use SymphonyElixir.TestSupport

  @moduletag :live_codex_observability
  @moduletag timeout: 600_000

  @result_file "HELLO_FROM_LIVE_CODEX.txt"
  @default_artifact_path Path.join(System.tmp_dir!(), "symphony-live-codex-observability-last.json")

  @live_codex_observability_skip_reason if(
                                          System.get_env("SYMPHONY_RUN_LIVE_CODEX_OBSERVABILITY") != "1",
                                          do: "set SYMPHONY_RUN_LIVE_CODEX_OBSERVABILITY=1 to enable the real Codex observability probe"
                                        )

  @tag skip: @live_codex_observability_skip_reason
  test "captures real codex app-server messages for dashboard transcript feasibility" do
    run_id = System.unique_integer([:positive])

    test_root =
      Path.join(System.tmp_dir!(), "symphony-elixir-live-codex-observability-#{run_id}")

    artifact_path = artifact_path()

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "OBS-#{run_id}")
      File.mkdir_p!(workspace)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_command: live_codex_command(),
        codex_approval_policy: "never",
        codex_turn_timeout_ms: 600_000,
        codex_stall_timeout_ms: 600_000
      )

      issue = %Issue{
        id: "issue-live-codex-observability-#{run_id}",
        identifier: "OBS-#{run_id}",
        title: "Live Codex observability probe",
        description: "Capture real Codex app-server traffic for dashboard feasibility assessment",
        state: "In Progress",
        url: "https://example.org/issues/OBS-#{run_id}",
        labels: ["observability"]
      }

      test_pid = self()
      on_message = fn message -> send(test_pid, {:app_server_message, message}) end

      assert {:ok, result} =
               AppServer.run(workspace, live_prompt(@result_file), issue, on_message: on_message)

      messages = drain_app_server_messages([])
      observed_methods = observed_methods(messages)
      result_path = Path.join(workspace, @result_file)
      result_contents = File.read!(result_path)

      artifact = %{
        run_id: run_id,
        recorded_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
        codex_command: live_codex_command(),
        workspace_root: workspace_root,
        workspace: workspace,
        result_file: result_path,
        result_file_contents: result_contents,
        event_count: length(messages),
        observed_events: Enum.map(messages, & &1.event),
        observed_methods: observed_methods,
        turn_session: normalize_term(result),
        messages: Enum.map(messages, &normalize_term/1)
      }

      File.write!(artifact_path, Jason.encode!(artifact, pretty: true))

      assert result.session_id == "#{result.thread_id}-#{result.turn_id}"
      assert String.trim_trailing(result_contents, "\n") == "hello from live codex observability probe"
      assert Enum.any?(messages, &(&1.event == :session_started))
      assert Enum.any?(messages, &(&1.event == :turn_completed))
      assert length(messages) >= 10
      assert Enum.member?(observed_methods, "turn/completed")
      assert Enum.member?(observed_methods, "item/agentMessage/delta")
      assert Enum.member?(observed_methods, "thread/tokenUsage/updated")
      assert Enum.any?(observed_methods, &(&1 in ["item/fileChange/outputDelta", "turn/diff/updated"]))

      assert Enum.any?(messages, fn message ->
               match?(%DateTime{}, message.timestamp)
             end)

      assert Enum.any?(messages, fn message ->
               raw = Map.get(message, :raw)
               is_binary(raw) and String.starts_with?(String.trim_leading(raw), "{")
             end)
    after
      File.rm_rf(test_root)
    end
  end

  defp artifact_path do
    System.get_env("SYMPHONY_LIVE_CODEX_OBSERVABILITY_ARTIFACT") || @default_artifact_path
  end

  defp live_codex_command do
    System.get_env("SYMPHONY_LIVE_CODEX_COMMAND") ||
      "codex --config shell_environment_policy.inherit=all --config model_reasoning_effort=xhigh --model gpt-5.4 app-server"
  end

  defp live_prompt(result_file) do
    """
    You are a live Codex observability probe.

    Hard rules:
    - Work only inside the current working directory.
    - Do not inspect git state.
    - Do not read unrelated files.
    - Do not browse the web.
    - Do not run extra commands beyond what is required below.

    Do exactly this:
    1. Create a file named #{result_file} with exactly this content:
       hello from live codex observability probe
    2. Print the file by running `cat #{result_file}`.
    3. Stop.
    """
  end

  defp drain_app_server_messages(messages) do
    receive do
      {:app_server_message, message} ->
        drain_app_server_messages([message | messages])
    after
      0 ->
        Enum.reverse(messages)
    end
  end

  defp observed_methods(messages) do
    messages
    |> Enum.map(fn message ->
      get_in(message, [:payload, "method"])
    end)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  defp normalize_term(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp normalize_term(value) when is_map(value) and not is_struct(value) do
    value
    |> Enum.map(fn {key, nested_value} -> {to_string(key), normalize_term(nested_value)} end)
    |> Enum.into(%{})
  end

  defp normalize_term(value) when is_list(value), do: Enum.map(value, &normalize_term/1)
  defp normalize_term(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_term(value), do: value
end
