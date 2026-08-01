defmodule SymphonyElixir.ExtensionsTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias SymphonyElixir.Linear.Adapter
  alias SymphonyElixir.Tracker.Memory

  @endpoint SymphonyElixirWeb.Endpoint

  defmodule FakeLinearClient do
    def fetch_issues_by_states(states) do
      send(self(), {:fetch_issues_by_states_called, states})
      {:ok, states}
    end

    def fetch_issues_by_ids(issue_ids) do
      send(self(), {:fetch_issues_by_ids_called, issue_ids})
      {:ok, issue_ids}
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

    def handle_call({:stop_issue, identifier}, _from, state) do
      result =
        case Keyword.get(state, :stop, {:error, :issue_not_found}) do
          stop when is_function(stop, 1) -> stop.(identifier)
          stop -> stop
        end

      {:reply, result, state}
    end

    def handle_call({:resume_issue, identifier, options}, _from, state) do
      if test_pid = Keyword.get(state, :test_pid) do
        send(test_pid, {:resume_issue_called, identifier, options})
      end

      result =
        case Keyword.get(state, :resume, {:error, :issue_not_found}) do
          resume when is_function(resume, 1) -> resume.(options)
          resume -> resume
        end

      {:reply, result, state}
    end

    def handle_call({:record_progress, identifier, attributes}, _from, state) do
      control_result(state, :progress, :record_progress_called, identifier, attributes)
    end

    def handle_call({:authorize_review, identifier, attributes}, _from, state) do
      control_result(state, :review, :authorize_review_called, identifier, attributes)
    end

    defp control_result(state, result_key, message, identifier, attributes) do
      if test_pid = Keyword.get(state, :test_pid) do
        send(test_pid, {message, identifier, attributes})
      end

      {:reply, Keyword.get(state, result_key, {:error, :issue_not_found}), state}
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
    control_token = System.get_env("SYMPHONY_CONTROL_TOKEN")

    on_exit(fn ->
      Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)

      if is_nil(control_token) do
        System.delete_env("SYMPHONY_CONTROL_TOKEN")
      else
        System.put_env("SYMPHONY_CONTROL_TOKEN", control_token)
      end
    end)

    :ok
  end

  test "workflow store reloads changes, keeps last good workflow, and falls back when stopped" do
    ensure_workflow_store_running()
    assert {:ok, %{prompt: "You are an agent for this repository."}} = Workflow.current()

    write_workflow_file!(Workflow.workflow_file_path(),
      prompt: "Second prompt",
      poll_interval_ms: 45_000
    )

    send(WorkflowStore, :poll)

    assert_eventually(fn ->
      match?({:ok, %{prompt: "Second prompt"}}, Workflow.current())
    end)

    good_settings = Config.settings!()
    assert good_settings.polling.interval_ms == 45_000

    File.write!(Workflow.workflow_file_path(), "---\ntracker: [\n---\nBroken prompt\n")
    assert {:error, _reason} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()

    File.write!(
      Workflow.workflow_file_path(),
      "---\npolling:\n  interval_ms: nope\n---\nTyped-invalid prompt\n"
    )

    assert {:error, {:invalid_workflow_config, message}} = WorkflowStore.force_reload()
    assert message =~ "polling.interval_ms"
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()
    assert Config.settings!().polling.interval_ms == good_settings.polling.interval_ms
    assert {:error, {:invalid_workflow_config, _message}} = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "linear",
      tracker_api_token: "token",
      tracker_project_slug: nil,
      prompt: "Semantic-invalid prompt"
    )

    assert {:error, :missing_linear_intake_scope} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()
    assert Config.settings!().polling.interval_ms == good_settings.polling.interval_ms
    assert {:error, :missing_linear_intake_scope} = Config.validate!()

    third_workflow = Path.join(Path.dirname(Workflow.workflow_file_path()), "THIRD_WORKFLOW.md")
    write_workflow_file!(third_workflow, prompt: "Third prompt")
    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, %{prompt: "Third prompt"}} = Workflow.current()

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)
    assert {:ok, %{prompt: "Third prompt"}} = WorkflowStore.current()
    assert {:ok, settings} = WorkflowStore.settings()
    assert settings.polling.interval_ms == 30_000
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
             WorkflowStore.settings()

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

    assert :ok = GenServer.stop(manual_pid)

    Workflow.set_workflow_file_path(existing_path)

    restart_result = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)

    assert match?({:ok, _pid}, restart_result) or
             match?({:error, {:already_started, _pid}}, restart_result)

    assert :ok = WorkflowStore.force_reload()
  end

  test "tracker delegates to memory and linear adapters" do
    issue = %Issue{id: "issue-1", identifier: "MT-1", state: "In Progress"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue, %{id: "ignored"}])
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    assert Config.settings!().tracker.kind == "memory"
    assert SymphonyElixir.Tracker.adapter() == Memory
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issues_by_states([" in progress ", 42])
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issues_by_ids(["issue-1"])

    binding = SymphonyElixir.Tracker.bind_agent_tools()
    assert binding.adapter == Memory
    assert binding.tool_specs == []
    assert binding.secret_environment_names == []

    assert SymphonyElixir.Tracker.execute_bound_agent_tool(binding, "not_a_memory_tool", %{})[
             "success"
           ] == false

    assert {:error, {:unsupported_tracker_kind, "future-tracker"}} =
             SymphonyElixir.Tracker.adapter_for_kind("future-tracker")

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "linear")
    assert SymphonyElixir.Tracker.adapter() == Adapter
    assert SymphonyElixir.Tracker.bind_agent_tools().secret_environment_names == ["LINEAR_API_KEY"]
  end

  test "linear adapter delegates reads and advertises its native agent tool" do
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)

    assert {:ok, ["Todo"]} = Adapter.fetch_issues_by_states(["Todo"])
    assert_receive {:fetch_issues_by_states_called, ["Todo"]}

    assert {:ok, ["issue-1"]} = Adapter.fetch_issues_by_ids(["issue-1"])
    assert_receive {:fetch_issues_by_ids_called, ["issue-1"]}

    assert [%{"name" => "linear_graphql"}] = Adapter.agent_tool_specs()
  end

  test "linear adapter requires one normalized intake scope" do
    assert {:ok, {:project_slug, "project"}} =
             Adapter.intake_scope(%{project_slug: " project ", team_key: nil})

    assert {:ok, {:team_key, "CC"}} =
             Adapter.intake_scope(%{project_slug: nil, team_key: " CC "})

    assert {:error, :missing_linear_intake_scope} =
             Adapter.intake_scope(%{project_slug: " ", team_key: nil})

    assert {:error, :conflicting_linear_intake_scope} =
             Adapter.intake_scope(%{project_slug: "project", team_key: "CC"})

    assert {:error, :invalid_linear_project_slug} =
             Adapter.intake_scope(%{project_slug: 123, team_key: nil})

    assert {:error, :invalid_linear_team_key} =
             Adapter.intake_scope(%{project_slug: nil, team_key: 123})

    assert {:error, :invalid_linear_project_slug} =
             Adapter.intake_scope(%{project_slug: "bad slug", team_key: nil})

    assert {:error, :invalid_linear_team_key} =
             Adapter.intake_scope(%{project_slug: nil, team_key: "CC\nOTHER"})
  end

  test "workflow store rejects a Linear intake scope hot reload" do
    ensure_workflow_store_running()

    assert {:ok, {:project_slug, "project"}} =
             Adapter.intake_scope(Config.settings!().tracker)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_project_slug: nil,
      tracker_team_key: "CC",
      prompt: "Team-scoped prompt"
    )

    assert {:error, :linear_intake_scope_restart_required} = WorkflowStore.force_reload()

    assert {:ok, {:project_slug, "project"}} =
             Adapter.intake_scope(Config.settings!().tracker)

    assert {:ok, %{prompt: current_prompt}} = Workflow.current()
    refute current_prompt == "Team-scoped prompt"

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      prompt: "Memory prompt"
    )

    assert :ok = WorkflowStore.force_reload()

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_project_slug: nil,
      tracker_team_key: "CC",
      prompt: "Two-step team prompt"
    )

    assert {:error, :linear_intake_scope_restart_required} = WorkflowStore.force_reload()
    assert Config.settings!().tracker.kind == "memory"

    write_workflow_file!(Workflow.workflow_file_path(), prompt: "Project-scoped prompt")
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Project-scoped prompt"}} = Workflow.current()
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

    conn = get(control_conn(), "/api/v1/state")
    state_payload = json_response(conn, 200)

    assert state_payload == %{
             "generated_at" => state_payload["generated_at"],
             "counts" => %{
               "running" => 1,
               "deferred" => 0,
               "retrying" => 1,
               "blocked" => 1,
               "held" => 0
             },
             "running" => [
               %{
                 "issue_id" => "issue-http",
                 "issue_identifier" => "MT-HTTP",
                 "issue_url" => "https://example.org/issues/MT-HTTP",
                 "state" => "In Progress",
                 "worker_host" => nil,
                 "workspace_path" => nil,
                 "session_id" => "thread-http",
                 "turn_count" => 7,
                 "last_event" => "notification",
                 "last_message" => "rendered",
                 "started_at" => state_payload["running"] |> List.first() |> Map.fetch!("started_at"),
                 "last_event_at" => nil,
                 "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}
               }
             ],
             "deferred" => [],
             "retrying" => [
               %{
                 "issue_id" => "issue-retry",
                 "issue_identifier" => "MT-RETRY",
                 "issue_url" => "https://example.org/issues/MT-RETRY",
                 "attempt" => 2,
                 "due_at" => state_payload["retrying"] |> List.first() |> Map.fetch!("due_at"),
                 "error" => "boom",
                 "worker_host" => nil,
                 "workspace_path" => nil
               }
             ],
             "blocked" => [
               %{
                 "issue_id" => "issue-blocked",
                 "issue_identifier" => "MT-BLOCKED",
                 "issue_url" => "https://example.org/issues/MT-BLOCKED",
                 "state" => "In Progress",
                 "error" => "codex turn requires operator input",
                 "worker_host" => "dm-dev2",
                 "workspace_path" => "/workspaces/MT-BLOCKED",
                 "session_id" => "thread-blocked",
                 "blocked_at" => state_payload["blocked"] |> List.first() |> Map.fetch!("blocked_at"),
                 "last_event" => "turn_input_required",
                 "last_message" => "turn blocked: waiting for user input",
                 "last_event_at" => state_payload["blocked"] |> List.first() |> Map.fetch!("last_event_at")
               }
             ],
             "held" => [],
             "codex_totals" => %{
               "input_tokens" => 4,
               "output_tokens" => 8,
               "total_tokens" => 12,
               "seconds_running" => 42.5
             },
             "rate_limits" => %{"primary" => %{"remaining" => 11}}
           }

    conn = get(control_conn(), "/api/v1/MT-HTTP")
    issue_payload = json_response(conn, 200)

    assert issue_payload == %{
             "issue_identifier" => "MT-HTTP",
             "issue_id" => "issue-http",
             "status" => "running",
             "workspace" => %{
               "path" => Path.join(Config.settings!().workspace.root, "MT-HTTP"),
               "host" => nil
             },
             "attempts" => %{"restart_count" => 0, "current_retry_attempt" => 0},
             "running" => %{
               "worker_host" => nil,
               "workspace_path" => nil,
               "session_id" => "thread-http",
               "turn_count" => 7,
               "state" => "In Progress",
               "started_at" => issue_payload["running"]["started_at"],
               "last_event" => "notification",
               "last_message" => "rendered",
               "last_event_at" => nil,
               "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}
             },
             "deferred" => nil,
             "retry" => nil,
             "blocked" => nil,
             "hold" => nil,
             "logs" => %{"codex_session_logs" => []},
             "recent_events" => [],
             "last_error" => nil,
             "tracked" => %{}
           }

    conn = get(control_conn(), "/api/v1/MT-RETRY")

    assert %{"status" => "retrying", "retry" => %{"attempt" => 2, "error" => "boom"}} =
             json_response(conn, 200)

    conn = get(control_conn(), "/api/v1/MT-BLOCKED")

    assert %{
             "status" => "blocked",
             "last_error" => "codex turn requires operator input",
             "blocked" => %{
               "session_id" => "thread-blocked",
               "state" => "In Progress",
               "error" => "codex turn requires operator input"
             }
           } = json_response(conn, 200)

    conn = get(control_conn(), "/api/v1/MT-MISSING")

    assert json_response(conn, 404) == %{
             "error" => %{"code" => "issue_not_found", "message" => "Issue not found"}
           }

    conn = post(build_conn(), "/api/v1/refresh", %{})

    assert %{"queued" => true, "coalesced" => false, "operations" => ["poll", "reconcile"]} =
             json_response(conn, 202)
  end

  test "phoenix observability api projects durable held issues in state and issue views" do
    held_at = DateTime.utc_now()

    hold = %{
      issue_id: "issue-held",
      identifier: "MT-HELD",
      reason: "manual_stop",
      limit: nil,
      observed_tokens: 42,
      issue_state: "In Progress",
      worker_host: "dm-dev2",
      workspace_path: "/workspaces/MT-HELD",
      cleanup_pending: false,
      held_at: held_at
    }

    orchestrator_name = Module.concat(__MODULE__, :HeldObservabilityApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: Map.put(static_snapshot(), :held, [hold])
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(control_conn(), "/api/v1/state"), 200)

    assert state_payload["counts"]["held"] == 1

    assert state_payload["held"] == [
             %{
               "issue_id" => "issue-held",
               "issue_identifier" => "MT-HELD",
               "reason" => "manual_stop",
               "limit" => nil,
               "observed_tokens" => 42,
               "issue_state" => "In Progress",
               "worker_host" => "dm-dev2",
               "workspace_path" => "/workspaces/MT-HELD",
               "cleanup_pending" => false,
               "held_at" => held_at |> DateTime.truncate(:second) |> DateTime.to_iso8601()
             }
           ]

    assert %{
             "issue_id" => "issue-held",
             "issue_identifier" => "MT-HELD",
             "status" => "held",
             "workspace" => %{"host" => "dm-dev2", "path" => "/workspaces/MT-HELD"},
             "running" => nil,
             "retry" => nil,
             "blocked" => nil,
             "hold" => %{
               "reason" => "manual_stop",
               "observed_tokens" => 42,
               "cleanup_pending" => false
             }
           } = json_response(get(control_conn(), "/api/v1/MT-HELD"), 200)
  end

  test "phoenix observability api keeps deferred issues visible in state and issue views" do
    started_at = DateTime.utc_now()

    deferred = %{
      issue_id: "issue-deferred",
      identifier: "MT-DEFERRED",
      state: "deferred_wait",
      worker_host: "dm-dev2",
      workspace_path: "/workspaces/MT-DEFERRED",
      started_at: started_at
    }

    orchestrator_name = Module.concat(__MODULE__, :DeferredObservabilityApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: Map.put(static_snapshot(), :deferred, [deferred])
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(control_conn(), "/api/v1/state"), 200)

    assert state_payload["counts"]["deferred"] == 1

    assert state_payload["deferred"] == [
             %{
               "issue_id" => "issue-deferred",
               "issue_identifier" => "MT-DEFERRED",
               "state" => "deferred_wait",
               "worker_host" => "dm-dev2",
               "workspace_path" => "/workspaces/MT-DEFERRED",
               "started_at" => started_at |> DateTime.truncate(:second) |> DateTime.to_iso8601()
             }
           ]

    assert %{
             "issue_id" => "issue-deferred",
             "issue_identifier" => "MT-DEFERRED",
             "status" => "deferred_wait",
             "workspace" => %{"host" => "dm-dev2", "path" => "/workspaces/MT-DEFERRED"},
             "running" => nil,
             "deferred" => %{
               "state" => "deferred_wait",
               "worker_host" => "dm-dev2",
               "workspace_path" => "/workspaces/MT-DEFERRED"
             },
             "retry" => nil,
             "blocked" => nil,
             "hold" => nil
           } = json_response(get(control_conn(), "/api/v1/MT-DEFERRED"), 200)
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

    assert json_response(get(build_conn(), "/unknown"), 404) ==
             %{"error" => %{"code" => "not_found", "message" => "Route not found"}}

    state_payload = json_response(get(control_conn(), "/api/v1/state"), 200)

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
  end

  test "phoenix control api requires an env-backed token and loopback access" do
    unknown_name = Module.concat(__MODULE__, :UnknownControlApiOrchestrator)

    {:ok, _pid} = StaticOrchestrator.start_link(name: unknown_name, snapshot: static_snapshot())
    start_test_endpoint(orchestrator: unknown_name, snapshot_timeout_ms: 50)
    System.delete_env("SYMPHONY_CONTROL_TOKEN")

    assert json_response(post(build_conn(), "/api/v1/UNKNOWN/stop", %{}), 503)["error"]["code"] ==
             "control_token_not_configured"

    assert json_response(get(build_conn(), "/api/v1/state"), 503)["error"]["code"] ==
             "control_token_not_configured"

    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    assert json_response(post(build_conn(), "/api/v1/UNKNOWN/stop", %{}), 401)["error"]["code"] ==
             "invalid_control_token"

    assert json_response(get(build_conn(), "/api/v1/state"), 401)["error"]["code"] ==
             "invalid_control_token"

    assert json_response(get(build_conn(), "/api/v1/MT-HTTP"), 401)["error"]["code"] ==
             "invalid_control_token"

    authorized_conn =
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")

    assert json_response(post(authorized_conn, "/api/v1/UNKNOWN/stop", %{}), 404)["error"]["code"] ==
             "issue_not_found"

    assert json_response(get(authorized_conn, "/api/v1/state"), 200)["counts"]["running"] == 1
    assert json_response(get(authorized_conn, "/api/v1/MT-HTTP"), 200)["status"] == "running"

    remote_conn =
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
      |> then(&%{&1 | remote_ip: {10, 0, 0, 1}})

    assert json_response(post(remote_conn, "/api/v1/UNKNOWN/resume", %{}), 403)["error"]["code"] ==
             "loopback_only"

    assert json_response(get(remote_conn, "/api/v1/state"), 403)["error"]["code"] ==
             "loopback_only"
  end

  test "phoenix stop api returns a durable held receipt" do
    orchestrator_name = Module.concat(__MODULE__, :StopControlApiOrchestrator)

    hold = %{
      issue_id: "issue-control",
      identifier: "MT-CONTROL",
      reason: "manual_stop",
      limit: nil,
      observed_tokens: 42,
      issue_state: "In Progress",
      worker_host: nil,
      workspace_path: "/tmp/MT-CONTROL",
      cleanup_pending: true,
      held_at: DateTime.utc_now()
    }

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        stop: {:ok, hold}
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    stop_payload =
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
      |> post("/api/v1/MT-CONTROL/stop", %{})
      |> json_response(200)

    assert stop_payload["status"] == "held"
    assert stop_payload["issue_identifier"] == "MT-CONTROL"
    assert stop_payload["hold"]["reason"] == "manual_stop"
    assert stop_payload["hold"]["observed_tokens"] == 42
    assert stop_payload["hold"]["cleanup_pending"] == true
  end

  test "phoenix resume api forwards a bounded phase and returns its effective receipt" do
    orchestrator_name = Module.concat(__MODULE__, :BoundedResumeControlApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        test_pid: self(),
        snapshot: static_snapshot(),
        resume:
          {:ok,
           %{
             issue_id: "issue-control",
             identifier: "MT-CONTROL",
             resumed: true,
             phase: "validation",
             requested_additional_input_tokens: 1_500,
             effective_additional_input_tokens: 1_000,
             current_issue_tier_limit: 1_000,
             attempt_input_token_baseline: 0,
             workspace_path: "/tmp/MT-CONTROL"
           }}
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    resume_payload =
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
      |> post("/api/v1/MT-CONTROL/resume", %{
        "phase" => "validation",
        "max_additional_input_tokens" => 1_500,
        "ignored" => "not-forwarded"
      })
      |> json_response(200)

    assert_received {:resume_issue_called, "MT-CONTROL",
                     %{
                       "phase" => "validation",
                       "max_additional_input_tokens" => 1_500
                     }}

    assert resume_payload == %{
             "issue_id" => "issue-control",
             "issue_identifier" => "MT-CONTROL",
             "status" => "resumed",
             "hold" => nil,
             "resume_phase" => "validation",
             "requested_additional_input_tokens" => 1_500,
             "effective_additional_input_tokens" => 1_000,
             "current_issue_tier_limit" => 1_000,
             "attempt_input_token_baseline" => 0,
             "workspace_path" => "/tmp/MT-CONTROL"
           }
  end

  test "phoenix control api forwards progress and review but denies deferred waits" do
    orchestrator_name = Module.concat(__MODULE__, :ProgressControlApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        test_pid: self(),
        snapshot: static_snapshot(),
        progress:
          {:ok,
           %{
             changed: true,
             progress_fingerprint: "progress-hash",
             review_fingerprint: "review-hash"
           }},
        review:
          {:ok,
           %{
             authorized: true,
             kind: "full",
             review_round_count: 1,
             review_fingerprint: "review-hash"
           }}
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    control_conn = fn ->
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
    end

    fingerprint = %{
      "contract_revision" => "v2",
      "head_sha" => "0123456789abcdef0123456789abcdef01234567"
    }

    progress_payload =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/progress", %{
        "fingerprint" => fingerprint,
        "progress_kind" => "workpad_checkpoint",
        "progress_receipt" => "linear-comment-1",
        "ignored" => "not-forwarded"
      })
      |> json_response(200)

    assert progress_payload == %{
             "changed" => true,
             "progress_fingerprint" => "progress-hash",
             "review_fingerprint" => "review-hash"
           }

    assert_received {:record_progress_called, "MT-CONTROL",
                     %{
                       "fingerprint" => ^fingerprint,
                       "progress_kind" => "workpad_checkpoint",
                       "progress_receipt" => "linear-comment-1"
                     }}

    head = "0123456789abcdef0123456789abcdef01234567"

    review_payload =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/review", %{
        "kind" => "full",
        "review_fingerprint" => "review-hash",
        "requested_head" => head,
        "observed_local_head" => head,
        "observed_remote_head" => head
      })
      |> json_response(200)

    assert review_payload["authorized"] == true
    assert review_payload["review_round_count"] == 1

    assert_received {:authorize_review_called, "MT-CONTROL",
                     %{
                       "kind" => "full",
                       "review_fingerprint" => "review-hash",
                       "requested_head" => ^head,
                       "observed_local_head" => ^head,
                       "observed_remote_head" => ^head
                     }}

    wait_error =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/wait", %{
        "expected_head" => head,
        "receipt_path" => "/tmp/MT-CONTROL/output/checks.jsonl",
        "timeout_seconds" => 1_200,
        "waiter_script" => "/tmp/MT-CONTROL/scripts/symphony/wait-for-pr-checks.mjs",
        "waiter_args" => [
          "--collect-terminal",
          "--head",
          head,
          "--output",
          "/tmp/MT-CONTROL/output/checks.jsonl"
        ]
      })
      |> json_response(422)

    assert wait_error == %{
             "error" => %{
               "code" => "deferred_wait_disabled",
               "message" => "Progress transition rejected: deferred_wait_disabled"
             }
           }

    refute_received {:register_deferred_wait_called, _, _}
  end

  test "phoenix resume api maps cleanup and hold persistence failures to safe responses" do
    orchestrator_name = Module.concat(__MODULE__, :FailedResumeControlApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        resume: fn options ->
          case options do
            %{"phase" => "validation"} -> {:error, :cleanup_failed}
            %{"phase" => "review-fix"} -> {:error, :progress_state_unavailable}
            _ -> {:error, :hold_state_unavailable}
          end
        end
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    control_conn = fn ->
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
    end

    cleanup_payload =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/resume", %{"phase" => "validation"})
      |> json_response(503)

    assert cleanup_payload["error"] == %{
             "code" => "cleanup_failed",
             "message" => "Cleanup could not be confirmed; the hold remains active"
           }

    hold_payload =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/resume", %{})
      |> json_response(503)

    assert hold_payload["error"] == %{
             "code" => "hold_state_unavailable",
             "message" => "Durable hold state is unavailable"
           }

    progress_payload =
      control_conn.()
      |> post("/api/v1/MT-CONTROL/resume", %{"phase" => "review-fix"})
      |> json_response(503)

    assert progress_payload["error"] == %{
             "code" => "progress_state_unavailable",
             "message" => "Durable progress state is unavailable"
           }
  end

  test "phoenix stop api maps cleanup and hold persistence failures to safe responses" do
    orchestrator_name = Module.concat(__MODULE__, :FailedStopControlApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        stop: fn
          "MT-CLEANUP" -> {:error, :cleanup_failed}
          "MT-HOLD" -> {:error, :hold_state_unavailable}
        end
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    control_conn = fn ->
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
    end

    cleanup_payload =
      control_conn.()
      |> post("/api/v1/MT-CLEANUP/stop", %{})
      |> json_response(503)

    assert cleanup_payload["error"] == %{
             "code" => "cleanup_failed",
             "message" => "The remote run could not be confirmed stopped"
           }

    hold_payload =
      control_conn.()
      |> post("/api/v1/MT-HOLD/stop", %{})
      |> json_response(503)

    assert hold_payload["error"] == %{
             "code" => "hold_state_unavailable",
             "message" => "Durable hold state is unavailable"
           }
  end

  test "phoenix resume api rejects missing invalid and unverifiable phase budgets" do
    orchestrator_name = Module.concat(__MODULE__, :RejectedBudgetResumeControlApiOrchestrator)

    resume = fn options ->
      case options do
        %{"phase" => "unknown"} ->
          {:error, :invalid_resume_phase}

        %{"phase" => "validation", "max_additional_input_tokens" => "many"} ->
          {:error, :invalid_max_additional_input_tokens}

        %{"phase" => "validation", "max_additional_input_tokens" => 100} ->
          {:error, :tracker_unavailable}

        %{"phase" => "validation"} ->
          {:error, :max_additional_input_tokens_required}

        _ ->
          {:error, :resume_phase_required}
      end
    end

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        resume: resume
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    for {body, expected_code, expected_status} <- [
          {%{}, "resume_phase_required", 422},
          {%{"phase" => "unknown"}, "invalid_resume_phase", 422},
          {%{"phase" => "validation"}, "max_additional_input_tokens_required", 422},
          {%{"phase" => "validation", "max_additional_input_tokens" => "many"}, "invalid_max_additional_input_tokens", 422},
          {%{"phase" => "validation", "max_additional_input_tokens" => 100}, "tracker_unavailable", 503}
        ] do
      payload =
        build_conn()
        |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
        |> post("/api/v1/MT-CONTROL/resume", body)
        |> json_response(expected_status)

      assert payload["error"]["code"] == expected_code
    end
  end

  test "phoenix observability api preserves snapshot timeout behavior" do
    timeout_orchestrator = Module.concat(__MODULE__, :TimeoutOrchestrator)
    {:ok, _pid} = SlowOrchestrator.start_link(name: timeout_orchestrator)
    start_test_endpoint(orchestrator: timeout_orchestrator, snapshot_timeout_ms: 1)

    timeout_payload = json_response(get(control_conn(), "/api/v1/state"), 200)

    assert timeout_payload ==
             %{
               "generated_at" => timeout_payload["generated_at"],
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
    assert html =~ ~r|/dashboard\.css\?v=[0-9a-f]{12}|

    assert html =~
             ~r|<link rel="icon" type="image/png" sizes="128x128" href="/favicon\.png\?v=[0-9a-f]{12}">|

    assert html =~ "/vendor/phoenix_html/phoenix_html.js"
    assert html =~ "/vendor/phoenix/phoenix.js"
    assert html =~ "/vendor/phoenix_live_view/phoenix_live_view.js"
    refute html =~ "/assets/app.js"
    refute html =~ "<style>"

    dashboard_css = response(get(build_conn(), "/dashboard.css"), 200)
    assert dashboard_css =~ ":root {"
    assert dashboard_css =~ ".status-badge-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-offline"
    assert dashboard_css =~ "text-decoration-thickness: 1px"

    favicon_conn = get(build_conn(), "/favicon.png")
    assert response(favicon_conn, 200) == File.read!("priv/static/favicon.png")
    assert Plug.Conn.get_resp_header(favicon_conn, "content-type") == ["image/png; charset=utf-8"]

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
    assert html =~ "MT-HTTP"
    assert html =~ "MT-RETRY"
    assert html =~ "MT-BLOCKED"
    assert html =~ ~s(href="https://example.org/issues/MT-HTTP")
    assert html =~ ~s(href="https://example.org/issues/MT-RETRY")
    assert html =~ ~s(href="https://example.org/issues/MT-BLOCKED")
    assert html =~ ~s(aria-label="Open MT-HTTP in the issue tracker")
    assert html =~ "rendered"
    assert html =~ "turn blocked: waiting for user input"
    assert html =~ "Runtime"
    assert html =~ "Live"
    assert html =~ "Offline"
    assert html =~ "Copy ID"
    assert html =~ "Codex update"
    refute html =~ "data-runtime-clock="
    refute html =~ "setInterval(refreshRuntimeClocks"
    refute html =~ "Refresh now"
    refute html =~ "Transport"
    assert html =~ "status-badge-live"
    assert html =~ "status-badge-offline"

    updated_snapshot =
      put_in(snapshot.running, [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          issue_url: "javascript:alert('nope')",
          state: "In Progress",
          session_id: "thread-http",
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
          started_at: DateTime.utc_now()
        }
      ])

    :sys.replace_state(orchestrator_pid, fn state ->
      Keyword.put(state, :snapshot, updated_snapshot)
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      render(view) =~ "agent message content streaming: structured update"
    end)

    refute render(view) =~ "javascript:alert"
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

    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    response =
      Req.get!("http://127.0.0.1:#{port}/api/v1/state",
        headers: [{"x-symphony-control-token", "test-control-token"}]
      )

    assert response.status == 200

    assert response.body["counts"] == %{
             "running" => 1,
             "deferred" => 0,
             "retrying" => 1,
             "blocked" => 1,
             "held" => 0
           }

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

  defp control_conn do
    System.put_env("SYMPHONY_CONTROL_TOKEN", "test-control-token")

    build_conn()
    |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
  end

  defp static_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          issue_url: "https://example.org/issues/MT-HTTP",
          state: "In Progress",
          session_id: "thread-http",
          turn_count: 7,
          codex_app_server_pid: nil,
          last_codex_message: "rendered",
          last_codex_timestamp: nil,
          last_codex_event: :notification,
          codex_input_tokens: 4,
          codex_output_tokens: 8,
          codex_total_tokens: 12,
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [
        %{
          issue_id: "issue-retry",
          identifier: "MT-RETRY",
          issue_url: "https://example.org/issues/MT-RETRY",
          attempt: 2,
          due_in_ms: 2_000,
          error: "boom"
        }
      ],
      blocked: [
        %{
          issue_id: "issue-blocked",
          identifier: "MT-BLOCKED",
          issue_url: "https://example.org/issues/MT-BLOCKED",
          state: "In Progress",
          error: "codex turn requires operator input",
          worker_host: "dm-dev2",
          workspace_path: "/workspaces/MT-BLOCKED",
          session_id: "thread-blocked",
          blocked_at: DateTime.utc_now(),
          last_codex_event: :turn_input_required,
          last_codex_message: %{
            event: :turn_input_required,
            message: %{"method" => "turn/input_required"},
            timestamp: DateTime.utc_now()
          },
          last_codex_timestamp: DateTime.utc_now()
        }
      ],
      codex_totals: %{input_tokens: 4, output_tokens: 8, total_tokens: 12, seconds_running: 42.5},
      rate_limits: %{"primary" => %{"remaining" => 11}}
    }
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
