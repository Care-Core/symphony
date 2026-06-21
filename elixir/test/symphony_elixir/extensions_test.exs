defmodule SymphonyElixir.ExtensionsTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias SymphonyElixir.Linear.Adapter
  alias SymphonyElixir.Tracker.Memory

  @endpoint SymphonyElixirWeb.Endpoint

  defmodule FakeLinearClient do
    def fetch_candidate_issues do
      send(self(), :fetch_candidate_issues_called)
      {:ok, [:candidate]}
    end

    def fetch_issues_by_states(states) do
      send(self(), {:fetch_issues_by_states_called, states})
      {:ok, states}
    end

    def fetch_issue_states_by_ids(issue_ids) do
      send(self(), {:fetch_issue_states_by_ids_called, issue_ids})
      {:ok, issue_ids}
    end

    def graphql(query, variables) do
      send(self(), {:graphql_called, query, variables})

      case Process.get({__MODULE__, :graphql_results}) do
        [result | rest] ->
          Process.put({__MODULE__, :graphql_results}, rest)
          result

        _ ->
          Process.get({__MODULE__, :graphql_result})
      end
    end
  end

  defmodule SlowOrchestrator do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def init(:ok), do: {:ok, :ok}

    def handle_call(:snapshot, _from, state) do
      Process.sleep(25)
      {:reply, %{}, state}
    end

    def handle_call(:request_refresh, _from, state) do
      {:reply, :unavailable, state}
    end
  end

  defmodule StaticOrchestrator do
    use GenServer

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts), do: {:ok, opts}

    def handle_call(:snapshot, _from, state) do
      {:reply, Keyword.fetch!(state, :snapshot), state}
    end

    def handle_call(:request_refresh, _from, state) do
      {:reply, Keyword.get(state, :refresh, :unavailable), state}
    end
  end

  setup do
    linear_client_module = Application.get_env(:symphony_elixir, :linear_client_module)

    on_exit(fn ->
      if is_nil(linear_client_module) do
        Application.delete_env(:symphony_elixir, :linear_client_module)
      else
        Application.put_env(:symphony_elixir, :linear_client_module, linear_client_module)
      end
    end)

    :ok
  end

  setup do
    endpoint_config = Application.get_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, [])

    on_exit(fn ->
      Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    end)

    :ok
  end

  setup do
    archive_root = Application.get_env(:symphony_elixir, :run_archive_root)

    on_exit(fn ->
      if is_nil(archive_root) do
        Application.delete_env(:symphony_elixir, :run_archive_root)
      else
        Application.put_env(:symphony_elixir, :run_archive_root, archive_root)
      end
    end)

    :ok
  end

  test "workflow store reloads changes, keeps last good workflow, and falls back when stopped" do
    ensure_workflow_store_running()
    assert {:ok, %{prompt: "You are an agent for this repository."}} = Workflow.current()

    write_workflow_file!(Workflow.workflow_file_path(), prompt: "Second prompt")
    send(WorkflowStore, :poll)

    assert_eventually(fn ->
      match?({:ok, %{prompt: "Second prompt"}}, Workflow.current())
    end)

    File.write!(Workflow.workflow_file_path(), "---\ntracker: [\n---\nBroken prompt\n")
    assert {:error, _reason} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()

    third_workflow = Path.join(Path.dirname(Workflow.workflow_file_path()), "THIRD_WORKFLOW.md")
    write_workflow_file!(third_workflow, prompt: "Third prompt")
    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, %{prompt: "Third prompt"}} = Workflow.current()

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)
    assert {:ok, %{prompt: "Third prompt"}} = WorkflowStore.current()
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, _pid} = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)
  end

  test "workflow store init stops on missing workflow file" do
    missing_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "MISSING_WORKFLOW.md")
    Workflow.set_workflow_file_path(missing_path)

    assert {:stop, {:missing_workflow_file, ^missing_path, :enoent}} = WorkflowStore.init([])
  end

  test "workflow store start_link and poll callback cover missing-file error paths" do
    ensure_workflow_store_running()
    existing_path = Workflow.workflow_file_path()
    manual_path = Path.join(Path.dirname(existing_path), "MANUAL_WORKFLOW.md")
    missing_path = Path.join(Path.dirname(existing_path), "MANUAL_MISSING_WORKFLOW.md")

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)

    Workflow.set_workflow_file_path(missing_path)

    assert {:error, {:missing_workflow_file, ^missing_path, :enoent}} =
             WorkflowStore.force_reload()

    write_workflow_file!(manual_path, prompt: "Manual workflow prompt")
    Workflow.set_workflow_file_path(manual_path)

    assert {:ok, manual_pid} = WorkflowStore.start_link()
    assert Process.alive?(manual_pid)

    state = :sys.get_state(manual_pid)
    File.write!(manual_path, "---\ntracker: [\n---\nBroken prompt\n")
    assert {:noreply, returned_state} = WorkflowStore.handle_info(:poll, state)
    assert returned_state.workflow.prompt == "Manual workflow prompt"
    refute returned_state.stamp == nil
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(missing_path)
    assert {:noreply, path_error_state} = WorkflowStore.handle_info(:poll, returned_state)
    assert path_error_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(manual_path)
    File.rm!(manual_path)
    assert {:noreply, removed_state} = WorkflowStore.handle_info(:poll, path_error_state)
    assert removed_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Process.exit(manual_pid, :normal)
    restart_result = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)

    assert match?({:ok, _pid}, restart_result) or
             match?({:error, {:already_started, _pid}}, restart_result)

    Workflow.set_workflow_file_path(existing_path)
    WorkflowStore.force_reload()
  end

  test "tracker delegates to memory and linear adapters" do
    issue = %Issue{id: "issue-1", identifier: "MT-1", state: "In Progress"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue, %{id: "ignored"}])
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    assert Config.settings!().tracker.kind == "memory"
    assert SymphonyElixir.Tracker.adapter() == Memory
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_candidate_issues()
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issues_by_states([" in progress ", 42])
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issue_states_by_ids(["issue-1"])
    assert :ok = SymphonyElixir.Tracker.create_comment("issue-1", "comment")
    assert :ok = SymphonyElixir.Tracker.update_issue_state("issue-1", "Done")
    assert_receive {:memory_tracker_comment, "issue-1", "comment"}
    assert_receive {:memory_tracker_state_update, "issue-1", "Done"}

    Application.delete_env(:symphony_elixir, :memory_tracker_recipient)
    assert :ok = Memory.create_comment("issue-1", "quiet")
    assert :ok = Memory.update_issue_state("issue-1", "Quiet")

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "linear")
    assert SymphonyElixir.Tracker.adapter() == Adapter
  end

  test "linear adapter delegates reads and validates mutation responses" do
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)

    assert {:ok, [:candidate]} = Adapter.fetch_candidate_issues()
    assert_receive :fetch_candidate_issues_called

    assert {:ok, ["Todo"]} = Adapter.fetch_issues_by_states(["Todo"])
    assert_receive {:fetch_issues_by_states_called, ["Todo"]}

    assert {:ok, ["issue-1"]} = Adapter.fetch_issue_states_by_ids(["issue-1"])
    assert_receive {:fetch_issue_states_by_ids_called, ["issue-1"]}

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => true}}}}
    )

    assert :ok = Adapter.create_comment("issue-1", "hello")
    assert_receive {:graphql_called, create_comment_query, %{body: "hello", issueId: "issue-1"}}
    assert create_comment_query =~ "commentCreate"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => false}}}}
    )

    assert {:error, :comment_create_failed} =
             Adapter.create_comment("issue-1", "broken")

    Process.put({FakeLinearClient, :graphql_result}, {:error, :boom})

    assert {:error, :boom} = Adapter.create_comment("issue-1", "boom")

    Process.put({FakeLinearClient, :graphql_result}, {:ok, %{"data" => %{}}})
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "weird")

    Process.put({FakeLinearClient, :graphql_result}, :unexpected)
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "odd")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => true}}}}
      ]
    )

    assert :ok = Adapter.update_issue_state("issue-1", "Done")
    assert_receive {:graphql_called, state_lookup_query, %{issueId: "issue-1", stateName: "Done"}}
    assert state_lookup_query =~ "states"

    assert_receive {:graphql_called, update_issue_query, %{issueId: "issue-1", stateId: "state-1"}}

    assert update_issue_query =~ "issueUpdate"

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => false}}}}
      ]
    )

    assert {:error, :issue_update_failed} =
             Adapter.update_issue_state("issue-1", "Broken")

    Process.put({FakeLinearClient, :graphql_results}, [{:error, :boom}])

    assert {:error, :boom} = Adapter.update_issue_state("issue-1", "Boom")

    Process.put({FakeLinearClient, :graphql_results}, [{:ok, %{"data" => %{}}}])
    assert {:error, :state_not_found} = Adapter.update_issue_state("issue-1", "Missing")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{}}}
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Weird")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        :unexpected
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Odd")
  end

  test "phoenix observability api preserves state, issue, and refresh responses" do
    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :ObservabilityApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll", "reconcile"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    conn = get(build_conn(), "/api/v1/state")
    state_payload = json_response(conn, 200)
    running_entry = state_payload["running"] |> List.first()

    assert state_payload["counts"] == %{"running" => 1, "retrying" => 1}
    assert state_payload["alerts"] == []
    assert state_payload["polling"] == %{"checking?" => false, "next_poll_in_ms" => nil, "poll_interval_ms" => nil}
    assert state_payload["rate_limits"] == %{"primary" => %{"remaining" => 11}}

    assert state_payload["recent_outcomes"] == [
             %{
               "issue_id" => "issue-done",
               "issue_identifier" => "MT-DONE",
               "issue_url" => "https://linear.app/care-core/issue/MT-DONE",
               "title" => "Completed issue",
               "outcome" => "completed_turn",
               "status" => "completed",
               "state" => "Completed",
               "session_id" => "thread-done-turn-1",
               "last_event" => "turn_completed",
               "last_message" => "turn completed",
               "runtime_seconds" => 12,
               "finished_at" => state_payload["recent_outcomes"] |> List.first() |> Map.fetch!("finished_at"),
               "tokens" => %{"input_tokens" => 3, "output_tokens" => 5, "total_tokens" => 8},
               "display_message" => state_payload["recent_outcomes"] |> List.first() |> Map.fetch!("display_message"),
               "display_message_preview" => state_payload["recent_outcomes"] |> List.first() |> Map.fetch!("display_message_preview"),
               "display_message_expandable" => true
             }
           ]

    assert state_payload["recent_outcomes"] |> List.first() |> Map.fetch!("display_message") =~ "Created the hello world file"

    assert running_entry["issue_id"] == "issue-http"
    assert running_entry["issue_identifier"] == "MT-HTTP"
    assert running_entry["issue_url"] == "https://linear.app/care-core/issue/MT-HTTP"
    assert running_entry["title"] == "Observe HTTP issue"
    assert running_entry["state"] == "In Progress"
    assert running_entry["session_id"] == "thread-http"
    assert running_entry["thread_id"] == "thread-http"
    assert running_entry["current_turn_id"] == "turn-http-7"
    assert running_entry["health"] == "healthy"
    assert running_entry["event_count"] == 2
    assert running_entry["turn_count"] == 7
    assert running_entry["last_event"] == "notification"
    assert running_entry["last_message"] == "rendered"
    assert running_entry["tokens"] == %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}
    assert is_number(running_entry["burn_rate_tokens_per_min"])

    conn = get(build_conn(), "/api/v1/MT-HTTP")
    issue_payload = json_response(conn, 200)

    assert issue_payload["issue_identifier"] == "MT-HTTP"
    assert issue_payload["issue_id"] == "issue-http"
    assert issue_payload["issue_url"] == "https://linear.app/care-core/issue/MT-HTTP"
    assert issue_payload["title"] == "Observe HTTP issue"
    assert issue_payload["status"] == "running"
    assert issue_payload["event_count"] == 2

    assert issue_payload["workspace"] == %{
             "path" => Path.join(Config.settings!().workspace.root, "MT-HTTP"),
             "host" => nil
           }

    assert issue_payload["running"]["thread_id"] == "thread-http"
    assert issue_payload["running"]["current_turn_id"] == "turn-http-7"
    assert issue_payload["running"]["health"] == "healthy"
    assert issue_payload["running"]["tokens"] == %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}

    assert Enum.map(issue_payload["recent_events"], & &1["method"]) == [
             "turn/started",
             "item/agentMessage/delta"
           ]

    conn = get(build_conn(), "/api/v1/runs/MT-HTTP")
    inspector_payload = json_response(conn, 200)

    assert inspector_payload["issue_identifier"] == "MT-HTTP"
    assert inspector_payload["event_count"] == 2
    assert inspector_payload["inspector_path"] == "/runs/MT-HTTP"

    assert inspector_payload["operator_transcript"] == [
             %{
               "kind" => "assistant",
               "title" => "Assistant",
               "body" => "rendered",
               "timestamp" => inspector_payload["operator_transcript"] |> List.first() |> Map.fetch!("timestamp"),
               "raw" => inspector_payload["operator_transcript"] |> List.first() |> Map.fetch!("raw")
             }
           ]

    conn = get(build_conn(), "/api/v1/runs/MT-HTTP/events")
    events_payload = json_response(conn, 200)

    assert events_payload["issue_identifier"] == "MT-HTTP"
    assert events_payload["issue_id"] == "issue-http"
    assert events_payload["next_cursor"] == nil
    assert length(events_payload["events"]) == 2

    assert Enum.map(events_payload["events"], & &1["method"]) == [
             "turn/started",
             "item/agentMessage/delta"
           ]

    conn = get(build_conn(), "/api/v1/MT-DONE")
    recent_issue_payload = json_response(conn, 200)

    assert recent_issue_payload["issue_identifier"] == "MT-DONE"
    assert recent_issue_payload["issue_id"] == "issue-done"
    assert recent_issue_payload["title"] == "Completed issue"
    assert recent_issue_payload["status"] == "completed"
    assert recent_issue_payload["event_count"] == 1
    assert recent_issue_payload["recent_outcome"]["thread_id"] == "thread-done-turn-1"
    assert recent_issue_payload["recent_outcome"]["current_turn_id"] == "turn-done-1"
    assert recent_issue_payload["recent_outcome"]["status"] == "completed"
    assert recent_issue_payload["recent_outcome"]["state"] == "Completed"
    assert recent_issue_payload["recent_outcome"]["outcome"] == "completed_turn"
    assert Enum.map(recent_issue_payload["recent_events"], & &1["method"]) == ["item/completed"]
    assert recent_issue_payload["operator_transcript"] |> List.first() |> Map.fetch!("body") =~ "Created the hello world file"

    conn = get(build_conn(), "/api/v1/runs/MT-DONE/events")
    recent_events_payload = json_response(conn, 200)

    assert recent_events_payload["issue_identifier"] == "MT-DONE"
    assert recent_events_payload["issue_id"] == "issue-done"
    assert recent_events_payload["status"] == "completed"
    assert recent_events_payload["next_cursor"] == nil
    assert Enum.map(recent_events_payload["events"], & &1["method"]) == ["item/completed"]

    limited_events_payload = json_response(get(build_conn(), "/api/v1/runs/MT-HTTP/events?limit=1"), 200)
    assert Enum.map(limited_events_payload["events"], & &1["method"]) == ["item/agentMessage/delta"]

    conn = get(build_conn(), "/api/v1/MT-RETRY")

    assert %{"status" => "retrying", "retry" => %{"attempt" => 2, "error" => "boom"}} =
             json_response(conn, 200)

    conn = get(build_conn(), "/api/v1/MT-MISSING")

    assert json_response(conn, 404) == %{
             "error" => %{"code" => "issue_not_found", "message" => "Issue not found"}
           }

    conn = get(build_conn(), "/api/v1/runs/MT-MISSING/events")

    assert json_response(conn, 404) == %{
             "error" => %{"code" => "issue_not_found", "message" => "Issue not found"}
           }

    conn = post(build_conn(), "/api/v1/refresh", %{})

    assert %{"queued" => true, "coalesced" => false, "operations" => ["poll", "reconcile"]} =
             json_response(conn, 202)
  end

  test "phoenix observability api and run inspector fall back to archived runs when live snapshot misses" do
    archive_root = Path.join(System.tmp_dir!(), "symphony-archive-#{System.unique_integer([:positive])}")
    Application.put_env(:symphony_elixir, :run_archive_root, archive_root)
    write_archived_run!(archive_root)

    on_exit(fn ->
      File.rm_rf(archive_root)
    end)

    orchestrator_name = Module.concat(__MODULE__, :ArchiveFallbackOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: %{
          running: [],
          retrying: [],
          codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
          rate_limits: %{"primary" => %{"remaining" => 11}},
          recent_outcomes: [],
          polling: %{checking?: false, next_poll_in_ms: nil, poll_interval_ms: nil}
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert state_payload["counts"] == %{"running" => 0, "retrying" => 0}

    assert state_payload["recent_outcomes"] == [
             %{
               "issue_id" => "issue-archive",
               "issue_identifier" => "MT-ARCHIVE",
               "issue_url" => "https://linear.app/care-core/issue/MT-ARCHIVE",
               "title" => "Archived issue",
               "outcome" => "completed_turn",
               "status" => "completed",
               "state" => "Completed",
               "session_id" => "thread-archive-turn-1",
               "last_event" => "turn_completed",
               "last_message" => "archived turn completed",
               "runtime_seconds" => 18,
               "finished_at" => state_payload["recent_outcomes"] |> List.first() |> Map.fetch!("finished_at"),
               "tokens" => %{"input_tokens" => 13, "output_tokens" => 21, "total_tokens" => 34},
               "display_message" => "Archived assistant summary",
               "display_message_preview" => "Archived assistant summary",
               "display_message_expandable" => false
             }
           ]

    archived_run = json_response(get(build_conn(), "/api/v1/runs/MT-ARCHIVE"), 200)

    assert archived_run["issue_identifier"] == "MT-ARCHIVE"
    assert archived_run["issue_id"] == "issue-archive"
    assert archived_run["title"] == "Archived issue"
    assert archived_run["status"] == "completed"
    assert archived_run["event_count"] == 2

    assert archived_run["workspace"] == %{
             "path" => Path.join(Config.settings!().workspace.root, "MT-ARCHIVE"),
             "host" => "spark"
           }

    assert archived_run["recent_outcome"]["thread_id"] == "thread-archive"
    assert archived_run["recent_outcome"]["current_turn_id"] == "turn-archive-1"
    assert archived_run["recent_outcome"]["status"] == "completed"
    assert archived_run["recent_outcome"]["state"] == "Completed"
    assert archived_run["recent_outcome"]["outcome"] == "completed_turn"
    assert Enum.map(archived_run["recent_events"], & &1["method"]) == ["item/completed", "item/completed"]

    assert archived_run["operator_transcript"] == [
             %{
               "kind" => "assistant",
               "title" => "Assistant",
               "body" => "Archived assistant summary",
               "timestamp" => archived_run["operator_transcript"] |> List.first() |> Map.fetch!("timestamp"),
               "raw" => archived_run["operator_transcript"] |> List.first() |> Map.fetch!("raw")
             },
             %{
               "kind" => "command",
               "title" => "Command",
               "body" => "git status --short",
               "timestamp" => archived_run["operator_transcript"] |> Enum.at(1) |> Map.fetch!("timestamp"),
               "raw" => archived_run["operator_transcript"] |> Enum.at(1) |> Map.fetch!("raw"),
               "meta" => "exit 0"
             },
             %{
               "kind" => "command_output",
               "title" => "Command output",
               "body" => "M archived.txt",
               "timestamp" => archived_run["operator_transcript"] |> List.last() |> Map.fetch!("timestamp"),
               "raw" => archived_run["operator_transcript"] |> List.last() |> Map.fetch!("raw")
             }
           ]

    archived_events = json_response(get(build_conn(), "/api/v1/runs/MT-ARCHIVE/events"), 200)
    assert archived_events["issue_identifier"] == "MT-ARCHIVE"
    assert archived_events["issue_id"] == "issue-archive"
    assert archived_events["status"] == "completed"
    assert Enum.map(archived_events["events"], & &1["method"]) == ["item/completed", "item/completed"]

    assert {:ok, _view, html} = live(build_conn(), "/runs/MT-ARCHIVE")
    assert html =~ "MT-ARCHIVE"
    assert html =~ "Archived issue"
    assert html =~ "Archived assistant summary"
    assert html =~ "git status --short"
    refute html =~ "Run unavailable"
  end

  test "phoenix observability api preserves 405, 404, and unavailable behavior" do
    unavailable_orchestrator = Module.concat(__MODULE__, :UnavailableOrchestrator)
    start_test_endpoint(orchestrator: unavailable_orchestrator, snapshot_timeout_ms: 5)

    assert json_response(post(build_conn(), "/api/v1/state", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/api/v1/refresh"), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/MT-1", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/runs/MT-1", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/runs/MT-1/events", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/unknown"), 404) ==
             %{"error" => %{"code" => "not_found", "message" => "Route not found"}}

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert state_payload ==
             %{
               "generated_at" => state_payload["generated_at"],
               "error" => %{"code" => "snapshot_unavailable", "message" => "Snapshot unavailable"}
             }

    assert json_response(post(build_conn(), "/api/v1/refresh", %{}), 503) ==
             %{
               "error" => %{
                 "code" => "orchestrator_unavailable",
                 "message" => "Orchestrator is unavailable"
               }
             }

    assert json_response(get(build_conn(), "/api/v1/runs/MT-HTTP"), 503) == %{
             "error" => %{"code" => "snapshot_unavailable", "message" => "Snapshot unavailable"}
           }

    assert json_response(get(build_conn(), "/api/v1/runs/MT-HTTP/events"), 503) == %{
             "error" => %{"code" => "snapshot_unavailable", "message" => "Snapshot unavailable"}
           }
  end

  test "phoenix observability api preserves snapshot timeout behavior" do
    timeout_orchestrator = Module.concat(__MODULE__, :TimeoutOrchestrator)
    {:ok, _pid} = SlowOrchestrator.start_link(name: timeout_orchestrator)
    start_test_endpoint(orchestrator: timeout_orchestrator, snapshot_timeout_ms: 1)

    timeout_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert timeout_payload ==
             %{
               "generated_at" => timeout_payload["generated_at"],
               "error" => %{"code" => "snapshot_timeout", "message" => "Snapshot timed out"}
             }

    assert json_response(get(build_conn(), "/api/v1/runs/MT-HTTP"), 504) == %{
             "error" => %{"code" => "snapshot_timeout", "message" => "Snapshot timed out"}
           }

    assert json_response(get(build_conn(), "/api/v1/runs/MT-HTTP/events"), 504) == %{
             "error" => %{"code" => "snapshot_timeout", "message" => "Snapshot timed out"}
           }
  end

  test "dashboard bootstraps liveview from embedded static assets" do
    orchestrator_name = Module.concat(__MODULE__, :AssetOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    html = html_response(get(build_conn(), "/"), 200)
    assert html =~ "/dashboard.css?v="
    refute html =~ "href=\"/dashboard.css\""
    refute Regex.match?(~r/href=\"\/dashboard\.css\?v=\d+\"/, html)
    assert Regex.match?(~r/href=\"\/dashboard\.css\?v=[0-9a-f]{8,}\"/, html)
    assert html =~ "/vendor/phoenix_html/phoenix_html.js"
    assert html =~ "/vendor/phoenix/phoenix.js"
    assert html =~ "/vendor/phoenix_live_view/phoenix_live_view.js"
    refute html =~ "/assets/app.js"
    refute html =~ "<style>"

    dashboard_css = response(get(build_conn(), "/dashboard.css"), 200)
    assert dashboard_css =~ ":root {"
    assert dashboard_css =~ "color-scheme: light"
    refute dashboard_css =~ "color-scheme: dark"
    assert dashboard_css =~ ".status-badge-live"
    assert dashboard_css =~ ".transcript-body"
    assert dashboard_css =~ "white-space: pre-wrap"
    assert dashboard_css =~ ".inspector-main-stack,"
    assert dashboard_css =~ "align-content: start;\n  min-width: 0;"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-offline"

    phoenix_html_js = response(get(build_conn(), "/vendor/phoenix_html/phoenix_html.js"), 200)
    assert phoenix_html_js =~ "phoenix.link.click"

    phoenix_js = response(get(build_conn(), "/vendor/phoenix/phoenix.js"), 200)
    assert phoenix_js =~ "var Phoenix = (() => {"

    live_view_js =
      response(get(build_conn(), "/vendor/phoenix_live_view/phoenix_live_view.js"), 200)

    assert live_view_js =~ "var LiveView = (() => {"
  end

  test "dashboard liveview renders and refreshes over pubsub" do
    orchestrator_name = Module.concat(__MODULE__, :DashboardOrchestrator)
    snapshot = static_snapshot()

    {:ok, orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, view, html} = live(build_conn(), "/")
    assert html =~ "Operations Dashboard"
    assert html =~ "symphony · observability"
    refute html =~ "atlas · symphony observability"
    assert html =~ "<p class=\"eyebrow\">Symphony</p>"
    assert html =~ "href=\"https://linear.app/care-core/issue/MT-HTTP\""
    assert html =~ "href=\"https://linear.app/care-core/issue/MT-DONE\""
    assert html =~ "Active Runs"
    assert html =~ "Recent Outcomes"
    assert html =~ "MT-HTTP"
    assert html =~ "MT-RETRY"
    assert html =~ "MT-DONE"
    assert html =~ "/runs/MT-DONE"
    assert html =~ "Completed issue"
    refute html =~ ">turn completed<"
    assert html =~ "Created the hello world file, posted the Linear note, and verified the artifact still exists for handoff."
    assert html =~ "Show more"
    assert html =~ "Observe HTTP issue"
    assert html =~ "Inspect"
    assert html =~ "Live"
    assert html =~ "Offline"
    assert html =~ "Health"

    updated_snapshot =
      put_in(snapshot.running, [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          url: "https://linear.app/care-core/issue/MT-HTTP",
          title: "Observe HTTP issue",
          state: "In Progress",
          session_id: "thread-http",
          thread_id: "thread-http",
          current_turn_id: "turn-http-8",
          turn_count: 8,
          last_codex_event: :notification,
          last_codex_message: %{
            event: :notification,
            message: %{
              payload: %{
                "method" => "codex/event/agent_message_content_delta",
                "params" => %{
                  "msg" => %{
                    "content" => "structured update"
                  }
                }
              }
            }
          },
          last_codex_timestamp: DateTime.utc_now(),
          codex_input_tokens: 10,
          codex_output_tokens: 12,
          codex_total_tokens: 22,
          recent_codex_events: [
            %{
              event_id: 3,
              event: :notification,
              method: "item/agentMessage/delta",
              category: "agent_message",
              summary: "structured update",
              timestamp: DateTime.utc_now(),
              session_id: "thread-http",
              thread_id: "thread-http",
              turn_id: "turn-http-8",
              item_id: "msg-http",
              raw: "{\"method\":\"item/agentMessage/delta\"}",
              payload: %{
                "method" => "item/agentMessage/delta",
                "params" => %{
                  "threadId" => "thread-http",
                  "turnId" => "turn-http-8",
                  "itemId" => "msg-http",
                  "delta" => "structured update"
                }
              }
            }
          ],
          started_at: DateTime.utc_now()
        }
      ])

    :sys.replace_state(orchestrator_pid, fn state ->
      Keyword.put(state, :snapshot, updated_snapshot)
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      render(view) =~ "structured update"
    end)

    {:ok, _inspect_view, inspect_html} = live(build_conn(), "/?inspect=MT-HTTP")
    assert inspect_html =~ "Quick view"
    assert inspect_html =~ "Raw payload"
  end

  test "run inspector liveview renders codex-style operator transcript" do
    orchestrator_name = Module.concat(__MODULE__, :RunInspectorOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: transcript_snapshot(),
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, _view, html} = live(build_conn(), "/runs/MT-TRANSCRIPT")
    assert html =~ "Latest Assistant Update"
    assert html =~ "Operator Transcript"
    assert html =~ "Recent Event Feed"
    assert html =~ "Copy Resume Cmd"
    assert html =~ "codex --yolo resume"
    assert html =~ "Copy Cmd"
    assert html =~ "data-copy-text=\"thread-transcript\""
    assert html =~ "Assistant"
    assert html =~ "I’ve created the file."
    assert html =~ "Command"
    assert html =~ "cat HELLO_FROM_SYMPHONY.txt"
    assert html =~ "hello from symphony live dashboard demo"
    assert html =~ "Tool call linear_graphql"
    assert html =~ "Tool response linear_graphql"
    assert html =~ "File change"
    assert html =~ "Open in Linear"
    assert html =~ "href=\"https://linear.app/care-core/issue/MT-TRANSCRIPT\""
    assert html =~ "phx-hook=\"PersistDetailsState\""
    assert html =~ "data-details-key=\"raw:"
    refute html =~ "agent message streaming:"
  end

  test "run payload compacts streamed agent-message deltas for the inspector feed" do
    orchestrator_name = Module.concat(__MODULE__, :CompactedInspectorFeedOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: compacted_feed_snapshot(),
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    payload = json_response(get(build_conn(), "/api/v1/runs/MT-COMPACT"), 200)

    assert Enum.map(payload["recent_events"], & &1["method"]) == [
             "item/agentMessage/delta",
             "item/completed"
           ]

    assert payload["recent_events"] |> List.first() |> Map.fetch!("summary") ==
             "agent message streaming: Move issue to Done"

    assert payload["recent_events"] |> List.first() |> Map.fetch!("message") ==
             "Move issue to Done"

    raw_payload = json_response(get(build_conn(), "/api/v1/runs/MT-COMPACT/events"), 200)

    assert Enum.map(raw_payload["events"], & &1["method"]) == [
             "item/agentMessage/delta",
             "item/agentMessage/delta",
             "item/agentMessage/delta",
             "item/completed"
           ]
  end

  test "run inspector liveview renders recent outcomes after completion" do
    orchestrator_name = Module.concat(__MODULE__, :RecentOutcomeInspectorOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, _view, html} = live(build_conn(), "/runs/MT-DONE")
    assert html =~ "Run Inspector"
    assert html =~ "Completed issue"
    assert html =~ "thread-done-turn-1"
    assert html =~ "turn-done-1"
    assert html =~ "Latest Assistant Update"
    assert html =~ "Operator Transcript"
    assert html =~ "Recent Event Feed"
    assert html =~ "Copy Resume Cmd"
    assert html =~ "Copy Cmd"
    assert html =~ "Created the hello world file"
    assert html =~ "phx-hook=\"PersistDetailsState\""
    assert html =~ "phx-hook=\"ClipboardCopy\""
  end

  test "dashboard liveview renders an unavailable state without crashing" do
    start_test_endpoint(
      orchestrator: Module.concat(__MODULE__, :MissingDashboardOrchestrator),
      snapshot_timeout_ms: 5
    )

    {:ok, _view, html} = live(build_conn(), "/")
    assert html =~ "Snapshot unavailable"
    assert html =~ "snapshot_unavailable"
  end

  test "http server serves embedded assets, accepts form posts, and rejects invalid hosts" do
    spec = HttpServer.child_spec(port: 0)
    assert spec.id == HttpServer
    assert spec.start == {HttpServer, :start_link, [[port: 0]]}

    assert :ignore = HttpServer.start_link(port: nil)
    assert HttpServer.bound_port() == nil

    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :BoundPortOrchestrator)

    refresh = %{
      queued: true,
      coalesced: false,
      requested_at: DateTime.utc_now(),
      operations: ["poll"]
    }

    server_opts = [
      host: "127.0.0.1",
      port: 0,
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50
    ]

    start_supervised!({StaticOrchestrator, name: orchestrator_name, snapshot: snapshot, refresh: refresh})

    start_supervised!({HttpServer, server_opts})

    port = wait_for_bound_port()
    assert port == HttpServer.bound_port()

    response = Req.get!("http://127.0.0.1:#{port}/api/v1/state")
    assert response.status == 200
    assert response.body["counts"] == %{"running" => 1, "retrying" => 1}

    dashboard_css = Req.get!("http://127.0.0.1:#{port}/dashboard.css")
    assert dashboard_css.status == 200
    assert dashboard_css.body =~ ":root {"

    phoenix_js = Req.get!("http://127.0.0.1:#{port}/vendor/phoenix/phoenix.js")
    assert phoenix_js.status == 200
    assert phoenix_js.body =~ "var Phoenix = (() => {"

    refresh_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/refresh",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert refresh_response.status == 202
    assert refresh_response.body["queued"] == true

    method_not_allowed_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/state",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert method_not_allowed_response.status == 405
    assert method_not_allowed_response.body["error"]["code"] == "method_not_allowed"

    assert {:error, _reason} = HttpServer.start_link(host: "bad host", port: 0)
  end

  defp start_test_endpoint(overrides) do
    endpoint_config =
      :symphony_elixir
      |> Application.get_env(SymphonyElixirWeb.Endpoint, [])
      |> Keyword.merge(server: false, secret_key_base: String.duplicate("s", 64))
      |> Keyword.merge(overrides)

    Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    start_supervised!({SymphonyElixirWeb.Endpoint, []})
  end

  defp transcript_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-transcript",
          identifier: "MT-TRANSCRIPT",
          url: "https://linear.app/care-core/issue/MT-TRANSCRIPT",
          title: "Codex transcript demo",
          state: "In Progress",
          session_id: "thread-transcript",
          thread_id: "thread-transcript",
          current_turn_id: "turn-transcript-1",
          turn_count: 1,
          codex_app_server_pid: nil,
          last_codex_message: "completed transcript demo",
          last_codex_timestamp: DateTime.utc_now(),
          last_codex_event: :notification,
          codex_input_tokens: 42,
          codex_output_tokens: 18,
          codex_total_tokens: 60,
          recent_codex_events: [
            %{
              event_id: 1,
              event: :notification,
              method: "item/completed",
              category: "system",
              summary: "item completed: agent message (msg-transcript-1)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-transcript",
              thread_id: "thread-transcript",
              turn_id: "turn-transcript-1",
              item_id: "msg-transcript-1",
              raw: "{\"method\":\"item/completed\",\"type\":\"agentMessage\"}",
              payload: %{
                "method" => "item/completed",
                "params" => %{
                  "threadId" => "thread-transcript",
                  "turnId" => "turn-transcript-1",
                  "item" => %{
                    "id" => "msg-transcript-1",
                    "type" => "agentMessage",
                    "text" => "I’ve created the file."
                  }
                }
              }
            },
            %{
              event_id: 2,
              event: :notification,
              method: "item/completed",
              category: "command",
              summary: "item completed: command execution (call-transcript-cmd, completed)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-transcript",
              thread_id: "thread-transcript",
              turn_id: "turn-transcript-1",
              item_id: "call-transcript-cmd",
              raw: "{\"method\":\"item/completed\",\"type\":\"commandExecution\"}",
              payload: %{
                "method" => "item/completed",
                "params" => %{
                  "threadId" => "thread-transcript",
                  "turnId" => "turn-transcript-1",
                  "item" => %{
                    "id" => "call-transcript-cmd",
                    "type" => "commandExecution",
                    "command" => "/bin/bash -lc 'cat HELLO_FROM_SYMPHONY.txt'",
                    "aggregatedOutput" => "hello from symphony live dashboard demo\n",
                    "exitCode" => 0,
                    "status" => "completed"
                  }
                }
              }
            },
            %{
              event_id: 3,
              event: :tool_call_completed,
              method: "item/tool/call",
              category: "tool",
              summary: "dynamic tool call completed (linear_graphql)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-transcript",
              thread_id: "thread-transcript",
              turn_id: "turn-transcript-1",
              item_id: "call-transcript-tool",
              raw: "{\"method\":\"item/tool/call\"}",
              payload: %{
                "method" => "item/tool/call",
                "params" => %{
                  "threadId" => "thread-transcript",
                  "turnId" => "turn-transcript-1",
                  "callId" => "call-transcript-tool",
                  "tool" => "linear_graphql",
                  "arguments" => %{
                    "query" => "mutation AddComment { commentCreate { success } }",
                    "variables" => %{"issueId" => "issue-transcript", "body" => "done"}
                  }
                }
              }
            },
            %{
              event_id: 4,
              event: :notification,
              method: "item/completed",
              category: "file_change",
              summary: "item completed: file change (call-transcript-file, completed)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-transcript",
              thread_id: "thread-transcript",
              turn_id: "turn-transcript-1",
              item_id: "call-transcript-file",
              raw: "{\"method\":\"item/completed\",\"type\":\"fileChange\"}",
              payload: %{
                "method" => "item/completed",
                "params" => %{
                  "threadId" => "thread-transcript",
                  "turnId" => "turn-transcript-1",
                  "item" => %{
                    "id" => "call-transcript-file",
                    "type" => "fileChange",
                    "status" => "completed",
                    "changes" => [
                      %{
                        "path" => "/tmp/symphony-linear-observability-demo/workspaces/MT-TRANSCRIPT/HELLO_FROM_SYMPHONY.txt",
                        "kind" => %{"type" => "add"},
                        "diff" => "hello from symphony live dashboard demo\n"
                      }
                    ]
                  }
                }
              }
            }
          ],
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [],
      codex_totals: %{input_tokens: 42, output_tokens: 18, total_tokens: 60, seconds_running: 9.0},
      rate_limits: %{"primary" => %{"remaining" => 11}},
      recent_outcomes: [],
      polling: %{checking?: false, next_poll_in_ms: nil, poll_interval_ms: nil}
    }
  end

  defp compacted_feed_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-compact",
          identifier: "MT-COMPACT",
          url: "https://linear.app/care-core/issue/MT-COMPACT",
          title: "Compacted event feed demo",
          state: "In Progress",
          session_id: "thread-compact",
          thread_id: "thread-compact",
          current_turn_id: "turn-compact-1",
          turn_count: 1,
          codex_app_server_pid: nil,
          last_codex_message: "Move issue to Done",
          last_codex_timestamp: DateTime.utc_now(),
          last_codex_event: :notification,
          codex_input_tokens: 10,
          codex_output_tokens: 4,
          codex_total_tokens: 14,
          recent_codex_events: [
            streamed_delta_event(1, "thread-compact", "turn-compact-1", "msg-compact", "Move "),
            streamed_delta_event(2, "thread-compact", "turn-compact-1", "msg-compact", "issue "),
            streamed_delta_event(3, "thread-compact", "turn-compact-1", "msg-compact", "to Done"),
            %{
              event_id: 4,
              event: :notification,
              method: "item/completed",
              category: "system",
              summary: "item completed: agent message (msg-compact)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-compact",
              thread_id: "thread-compact",
              turn_id: "turn-compact-1",
              item_id: "msg-compact",
              raw: "{\"method\":\"item/completed\",\"type\":\"agentMessage\"}",
              payload: %{
                "method" => "item/completed",
                "params" => %{
                  "threadId" => "thread-compact",
                  "turnId" => "turn-compact-1",
                  "item" => %{
                    "id" => "msg-compact",
                    "type" => "agentMessage",
                    "text" => "Move issue to Done"
                  }
                }
              }
            }
          ],
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [],
      codex_totals: %{input_tokens: 10, output_tokens: 4, total_tokens: 14, seconds_running: 3.0},
      rate_limits: %{"primary" => %{"remaining" => 11}},
      recent_outcomes: [],
      polling: %{checking?: false, next_poll_in_ms: nil, poll_interval_ms: nil}
    }
  end

  defp streamed_delta_event(event_id, thread_id, turn_id, item_id, delta) do
    %{
      event_id: event_id,
      event: :notification,
      method: "item/agentMessage/delta",
      category: "agent_message",
      summary: "agent message streaming: #{delta}",
      timestamp: DateTime.utc_now(),
      session_id: thread_id,
      thread_id: thread_id,
      turn_id: turn_id,
      item_id: item_id,
      raw: "{\"method\":\"item/agentMessage/delta\"}",
      payload: %{
        "method" => "item/agentMessage/delta",
        "params" => %{
          "threadId" => thread_id,
          "turnId" => turn_id,
          "itemId" => item_id,
          "delta" => delta
        }
      }
    }
  end

  defp static_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          url: "https://linear.app/care-core/issue/MT-HTTP",
          title: "Observe HTTP issue",
          state: "In Progress",
          session_id: "thread-http",
          thread_id: "thread-http",
          current_turn_id: "turn-http-7",
          turn_count: 7,
          codex_app_server_pid: nil,
          last_codex_message: "rendered",
          last_codex_timestamp: DateTime.utc_now(),
          last_codex_event: :notification,
          codex_input_tokens: 4,
          codex_output_tokens: 8,
          codex_total_tokens: 12,
          recent_codex_events: [
            %{
              event_id: 1,
              event: :notification,
              method: "turn/started",
              category: "lifecycle",
              summary: "turn started",
              timestamp: DateTime.utc_now(),
              session_id: "thread-http",
              thread_id: "thread-http",
              turn_id: "turn-http-7",
              item_id: nil,
              raw: "{\"method\":\"turn/started\"}",
              payload: %{
                "method" => "turn/started",
                "params" => %{
                  "threadId" => "thread-http",
                  "turn" => %{"id" => "turn-http-7", "status" => "inProgress"}
                }
              }
            },
            %{
              event_id: 2,
              event: :notification,
              method: "item/agentMessage/delta",
              category: "agent_message",
              summary: "rendered",
              timestamp: DateTime.utc_now(),
              session_id: "thread-http",
              thread_id: "thread-http",
              turn_id: "turn-http-7",
              item_id: "msg-http",
              raw: "{\"method\":\"item/agentMessage/delta\"}",
              payload: %{
                "method" => "item/agentMessage/delta",
                "params" => %{
                  "threadId" => "thread-http",
                  "turnId" => "turn-http-7",
                  "itemId" => "msg-http",
                  "delta" => "rendered"
                }
              }
            }
          ],
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [
        %{
          issue_id: "issue-retry",
          identifier: "MT-RETRY",
          attempt: 2,
          due_in_ms: 2_000,
          error: "boom"
        }
      ],
      codex_totals: %{input_tokens: 4, output_tokens: 8, total_tokens: 12, seconds_running: 42.5},
      rate_limits: %{"primary" => %{"remaining" => 11}},
      recent_outcomes: [
        %{
          issue_id: "issue-done",
          identifier: "MT-DONE",
          url: "https://linear.app/care-core/issue/MT-DONE",
          title: "Completed issue",
          outcome: "completed_turn",
          session_id: "thread-done-turn-1",
          thread_id: "thread-done-turn-1",
          current_turn_id: "turn-done-1",
          worker_host: "spark",
          workspace_path: Path.join(Config.settings!().workspace.root, "MT-DONE"),
          last_event: :turn_completed,
          last_message: "turn completed",
          last_event_at: DateTime.utc_now(),
          runtime_seconds: 12,
          finished_at: DateTime.utc_now(),
          tokens: %{input_tokens: 3, output_tokens: 5, total_tokens: 8},
          recent_codex_events: [
            %{
              event_id: 9,
              event: :notification,
              method: "item/completed",
              category: "agent_message",
              summary: "item completed: agent message (msg-done)",
              timestamp: DateTime.utc_now(),
              session_id: "thread-done-turn-1",
              thread_id: "thread-done-turn-1",
              turn_id: "turn-done-1",
              item_id: "msg-done",
              raw: "{\"method\":\"item/completed\",\"type\":\"agentMessage\"}",
              payload: %{
                "method" => "item/completed",
                "params" => %{
                  "threadId" => "thread-done-turn-1",
                  "turnId" => "turn-done-1",
                  "item" => %{
                    "id" => "msg-done",
                    "type" => "agentMessage",
                    "text" =>
                      "Created the hello world file, posted the Linear note, and verified the artifact still exists for handoff. This final summary is intentionally long so the dashboard recent-outcomes table can show a truncated preview with an expandable full message."
                  }
                }
              }
            }
          ]
        }
      ],
      polling: %{checking?: false, next_poll_in_ms: nil, poll_interval_ms: nil}
    }
  end

  defp write_archived_run!(archive_root) do
    run_id = "run-archive-1"
    run_dir = Path.join([archive_root, "runs", run_id])
    issues_dir = Path.join(archive_root, "issues")
    File.mkdir_p!(run_dir)
    File.mkdir_p!(issues_dir)

    timestamps = [
      "2026-04-20T22:19:39Z",
      "2026-04-20T22:19:40Z"
    ]

    events = [
      %{
        "event_id" => 1,
        "issue_identifier" => "MT-ARCHIVE",
        "at" => Enum.at(timestamps, 0),
        "event" => "notification",
        "method" => "item/completed",
        "category" => "agent_message",
        "summary" => "item completed: agent message (msg-archive)",
        "message" => "item completed: agent message (msg-archive)",
        "session_id" => "thread-archive-turn-1",
        "thread_id" => "thread-archive",
        "turn_id" => "turn-archive-1",
        "item_id" => "msg-archive",
        "raw" => "{\"method\":\"item/completed\",\"type\":\"agentMessage\"}",
        "payload" => %{
          "method" => "item/completed",
          "params" => %{
            "threadId" => "thread-archive",
            "turnId" => "turn-archive-1",
            "item" => %{
              "id" => "msg-archive",
              "type" => "agentMessage",
              "text" => "Archived assistant summary"
            }
          }
        }
      },
      %{
        "event_id" => 2,
        "issue_identifier" => "MT-ARCHIVE",
        "at" => Enum.at(timestamps, 1),
        "event" => "notification",
        "method" => "item/completed",
        "category" => "command",
        "summary" => "item completed: command execution (cmd-archive)",
        "message" => "item completed: command execution (cmd-archive)",
        "session_id" => "thread-archive-turn-1",
        "thread_id" => "thread-archive",
        "turn_id" => "turn-archive-1",
        "item_id" => "cmd-archive",
        "raw" => "{\"method\":\"item/completed\",\"type\":\"commandExecution\"}",
        "payload" => %{
          "method" => "item/completed",
          "params" => %{
            "threadId" => "thread-archive",
            "turnId" => "turn-archive-1",
            "item" => %{
              "id" => "cmd-archive",
              "type" => "commandExecution",
              "command" => "git status --short",
              "aggregatedOutput" => "M archived.txt",
              "exitCode" => 0
            }
          }
        }
      }
    ]

    File.write!(
      Path.join(run_dir, "events.jsonl"),
      Enum.map_join(events, "", &(Jason.encode!(&1) <> "\n"))
    )

    File.write!(
      Path.join(run_dir, "summary.json"),
      Jason.encode!(
        %{
          run_id: run_id,
          issue_id: "issue-archive",
          issue_identifier: "MT-ARCHIVE",
          issue_url: "https://linear.app/care-core/issue/MT-ARCHIVE",
          title: "Archived issue",
          outcome: "completed_turn",
          session_id: "thread-archive-turn-1",
          thread_id: "thread-archive",
          current_turn_id: "turn-archive-1",
          worker_host: "spark",
          workspace_path: Path.join(Config.settings!().workspace.root, "MT-ARCHIVE"),
          turn_count: 1,
          started_at: "2026-04-20T22:19:21Z",
          finished_at: "2026-04-20T22:19:39Z",
          last_event: "turn_completed",
          last_message: "archived turn completed",
          runtime_seconds: 18,
          tokens: %{input_tokens: 13, output_tokens: 21, total_tokens: 34},
          operator_transcript: [
            %{
              kind: "assistant",
              title: "Assistant",
              body: "Archived assistant summary",
              timestamp: Enum.at(timestamps, 0),
              raw: "{\"method\":\"item/completed\",\"type\":\"agentMessage\"}"
            },
            %{
              kind: "command",
              title: "Command",
              body: "git status --short",
              timestamp: Enum.at(timestamps, 1),
              raw: "{\"method\":\"item/completed\",\"type\":\"commandExecution\"}",
              meta: "exit 0"
            },
            %{
              kind: "command_output",
              title: "Command output",
              body: "M archived.txt",
              timestamp: Enum.at(timestamps, 1),
              raw: "{\"method\":\"item/completed\",\"type\":\"commandExecution\"}"
            }
          ]
        },
        pretty: true
      )
    )

    File.write!(
      Path.join(issues_dir, "MT-ARCHIVE.json"),
      Jason.encode!(
        %{
          issue_identifier: "MT-ARCHIVE",
          issue_id: "issue-archive",
          issue_url: "https://linear.app/care-core/issue/MT-ARCHIVE",
          title: "Archived issue",
          outcome: "completed_turn",
          run_id: run_id,
          latest_session_id: "thread-archive-turn-1",
          updated_at: "2026-04-20T22:19:39Z"
        },
        pretty: true
      )
    )
  end

  defp wait_for_bound_port do
    assert_eventually(fn ->
      is_integer(HttpServer.bound_port())
    end)

    HttpServer.bound_port()
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")

  defp ensure_workflow_store_running do
    if Process.whereis(WorkflowStore) do
      :ok
    else
      case Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end
end
