defmodule SymphonyElixir.HoldStoreTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SymphonyElixir.HoldStore

  @state_filename ".symphony-holds.json"

  test "load returns an empty store when no state file exists" do
    workspace_root = workspace_root("missing")

    assert {:ok, %{}} = HoldStore.load(workspace_root)
    assert File.dir?(workspace_root)
  end

  test "persist writes a private, stably ordered store that load decodes" do
    workspace_root = workspace_root("round-trip")
    state_path = state_path(workspace_root)
    earlier = datetime!("2026-07-20T10:00:00.000000Z")
    later = datetime!("2026-07-21T11:30:00.000000Z")

    holds = %{
      "issue-b" => hold("issue-b", "SYM-2", later),
      "issue-a" =>
        hold("issue-a", "SYM-1", earlier,
          limit: nil,
          issue_state: nil,
          worker_host: nil,
          workspace_path: nil
        )
    }

    assert :ok = HoldStore.persist(workspace_root, holds)
    assert Bitwise.band(File.stat!(state_path).mode, 0o777) == 0o600

    assert %{
             "version" => 1,
             "holds" => [
               %{"issue_id" => "issue-a"},
               %{"issue_id" => "issue-b"}
             ]
           } = state_path |> File.read!() |> Jason.decode!()

    assert {:ok, ^holds} = HoldStore.load(workspace_root)
  end

  test "load defaults cleanup proof for legacy holds" do
    workspace_root = workspace_root("legacy-cleanup-proof")

    legacy_hold = Map.drop(encoded_hold(), ["cleanup_pending", "codex_app_server_pid"])
    write_json_state!(workspace_root, %{"version" => 1, "holds" => [legacy_hold]})

    assert {:ok,
            %{
              "issue-a" => %{
                cleanup_pending: false,
                codex_app_server_pid: nil
              }
            }} = HoldStore.load(workspace_root)
  end

  test "load rejects corrupt JSON" do
    workspace_root = workspace_root("corrupt-json")
    state_path = write_state!(workspace_root, "not-json")

    assert {:error, {:hold_state_invalid, ^state_path, %Jason.DecodeError{}}} =
             HoldStore.load(workspace_root)
  end

  test "load rejects the wrong schema and version" do
    workspace_root = workspace_root("schema")
    state_path = state_path(workspace_root)

    for payload <- [
          %{"version" => 2, "holds" => []},
          %{"version" => 1, "holds" => %{}},
          %{"version" => 1},
          []
        ] do
      write_json_state!(workspace_root, payload)

      assert {:error, {:hold_state_invalid, ^state_path, :invalid_schema}} =
               HoldStore.load(workspace_root)
    end
  end

  test "load rejects duplicate issue IDs" do
    workspace_root = workspace_root("duplicate")
    state_path = state_path(workspace_root)
    encoded_hold = encoded_hold()

    write_json_state!(workspace_root, %{"version" => 1, "holds" => [encoded_hold, encoded_hold]})

    assert {:error, {:hold_state_invalid, ^state_path, :duplicate_issue_id}} =
             HoldStore.load(workspace_root)
  end

  test "load rejects non-map and incomplete holds" do
    workspace_root = workspace_root("invalid-hold")
    state_path = state_path(workspace_root)

    for encoded_hold <- [42, Map.delete(encoded_hold(), "reason")] do
      write_json_state!(workspace_root, %{"version" => 1, "holds" => [encoded_hold]})

      assert {:error, {:hold_state_invalid, ^state_path, :invalid_hold}} =
               HoldStore.load(workspace_root)
    end
  end

  test "load rejects every invalid hold field type and value" do
    workspace_root = workspace_root("invalid-fields")
    state_path = state_path(workspace_root)

    invalid_fields = [
      {:issue_id, "issue_id", ""},
      {:issue_id, "issue_id", 1},
      {:identifier, "identifier", ""},
      {:identifier, "identifier", []},
      {:reason, "reason", ""},
      {:reason, "reason", %{}},
      {:limit, "limit", 0},
      {:limit, "limit", -1},
      {:limit, "limit", "1"},
      {:observed_tokens, "observed_tokens", -1},
      {:observed_tokens, "observed_tokens", 1.5},
      {:issue_state, "issue_state", 1},
      {:worker_host, "worker_host", false},
      {:workspace_path, "workspace_path", []},
      {:held_at, "held_at", ""},
      {:held_at, "held_at", 1}
    ]

    for {field, encoded_field, invalid_value} <- invalid_fields do
      invalid_hold = Map.put(encoded_hold(), encoded_field, invalid_value)
      write_json_state!(workspace_root, %{"version" => 1, "holds" => [invalid_hold]})

      assert {:error, {:hold_state_invalid, ^state_path, {:invalid_field, ^field}}} =
               HoldStore.load(workspace_root)
    end
  end

  test "load rejects malformed and non-UTC held-at times" do
    workspace_root = workspace_root("invalid-held-at")
    state_path = state_path(workspace_root)

    invalid_times = [
      {"not-a-time", :invalid_format},
      {"2026-07-21T12:00:00+01:00", :invalid_held_at}
    ]

    for {invalid_time, expected_reason} <- invalid_times do
      invalid_hold = Map.put(encoded_hold(), "held_at", invalid_time)
      write_json_state!(workspace_root, %{"version" => 1, "holds" => [invalid_hold]})

      assert {:error, {:hold_state_invalid, ^state_path, ^expected_reason}} =
               HoldStore.load(workspace_root)
    end
  end

  test "load rejects insecure state-file permissions" do
    workspace_root = workspace_root("permissions")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    File.chmod!(state_path, 0o644)

    assert {:error, {:hold_state_insecure_permissions, ^state_path, 0o644}} =
             HoldStore.load(workspace_root)
  end

  test "load rejects a non-regular state path" do
    workspace_root = workspace_root("non-regular")
    state_path = state_path(workspace_root)
    File.mkdir_p!(state_path)

    assert {:error, {:hold_state_invalid_file, ^state_path, :directory}} =
             HoldStore.load(workspace_root)
  end

  test "descriptor validation rejects a stat failure" do
    workspace_root = workspace_root("descriptor-stat")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    {:ok, io_device} = File.open(state_path, [:read, :binary])
    :ok = File.close(io_device)

    assert {:error, {:hold_state_descriptor_stat_failed, ^state_path, :terminated}} =
             HoldStore.read_validated_descriptor(io_device, state_path, initial_stat)
  end

  test "descriptor validation rejects permissions changed after the initial stat" do
    workspace_root = workspace_root("descriptor-permissions")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    File.chmod!(state_path, 0o644)
    {:ok, io_device} = File.open(state_path, [:read, :binary])
    on_exit(fn -> File.close(io_device) end)

    assert {:error, {:hold_state_insecure_descriptor, ^state_path, {:insecure_permissions, 0o644}}} =
             HoldStore.read_validated_descriptor(io_device, state_path, initial_stat)
  end

  test "descriptor validation rejects an identity mismatch" do
    workspace_root = workspace_root("descriptor-identity")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    replacement_path = write_state!(workspace_root, "replacement", "{}")
    {:ok, io_device} = File.open(replacement_path, [:read, :binary])
    on_exit(fn -> File.close(io_device) end)

    assert {:error, {:hold_state_identity_mismatch, ^state_path}} =
             HoldStore.read_validated_descriptor(io_device, state_path, initial_stat)
  end

  test "descriptor validation surfaces a read failure" do
    workspace_root = workspace_root("descriptor-read")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    {:ok, io_device} = File.open(state_path, [:write, :binary])
    on_exit(fn -> File.close(io_device) end)

    assert {:error, {:hold_state_descriptor_read_failed, ^state_path, :ebadf}} =
             HoldStore.read_validated_descriptor(io_device, state_path, initial_stat)
  end

  test "descriptor close failure after a successful read fails closed" do
    workspace_root = workspace_root("descriptor-close")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)

    descriptor_closer = fn io_device ->
      :ok = File.close(io_device)
      {:error, :simulated_close_failure}
    end

    assert {:error, {:hold_state_descriptor_close_failed, ^state_path, :simulated_close_failure}} =
             HoldStore.read_open_state_file_for_test(
               state_path,
               initial_stat,
               &HoldStore.read_validated_descriptor/3,
               descriptor_closer
             )
  end

  test "descriptor read and close errors are both preserved" do
    workspace_root = workspace_root("descriptor-read-and-close")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)

    descriptor_reader = fn _io_device, _state_path, _initial_stat ->
      {:error, :simulated_read_failure}
    end

    descriptor_closer = fn io_device ->
      :ok = File.close(io_device)
      {:error, :simulated_close_failure}
    end

    read_reason = :simulated_read_failure
    close_reason = :simulated_close_failure

    expected =
      {:error, {:hold_state_descriptor_read_and_close_failed, state_path, read_reason, close_reason}}

    assert expected ==
             HoldStore.read_open_state_file_for_test(
               state_path,
               initial_stat,
               descriptor_reader,
               descriptor_closer
             )
  end

  test "descriptor close is attempted without replacing a read exception or its stack" do
    workspace_root = workspace_root("descriptor-read-raise")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    test_pid = self()

    descriptor_reader = fn _io_device, _state_path, _initial_stat ->
      raise_descriptor_read_error()
    end

    descriptor_closer = fn io_device ->
      send(test_pid, :descriptor_close_attempted)
      File.close(io_device)
    end

    {exception, stacktrace} =
      try do
        HoldStore.read_open_state_file_for_test(
          state_path,
          initial_stat,
          descriptor_reader,
          descriptor_closer
        )

        flunk("expected descriptor reader to raise")
      rescue
        exception -> {exception, __STACKTRACE__}
      end

    assert %RuntimeError{message: "simulated descriptor read failure"} = exception
    assert_received :descriptor_close_attempted

    assert Enum.any?(stacktrace, fn
             {__MODULE__, :raise_descriptor_read_error, 0, _location} -> true
             _frame -> false
           end)
  end

  test "descriptor close failure is logged without replacing a raised read or its stack" do
    workspace_root = workspace_root("descriptor-read-raise-close-failure")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    initial_stat = File.lstat!(state_path)
    test_pid = self()

    descriptor_reader = fn _io_device, _state_path, _initial_stat ->
      raise_descriptor_read_error()
    end

    descriptor_closer = fn io_device ->
      :ok = File.close(io_device)
      {:error, :simulated_close_failure}
    end

    log =
      capture_log(fn ->
        try do
          HoldStore.read_open_state_file_for_test(
            state_path,
            initial_stat,
            descriptor_reader,
            descriptor_closer
          )
        rescue
          exception -> send(test_pid, {:raised_read, exception, __STACKTRACE__})
        end
      end)

    assert_receive {:raised_read, exception, stacktrace}
    assert %RuntimeError{message: "simulated descriptor read failure"} = exception

    assert Enum.any?(stacktrace, fn
             {__MODULE__, :raise_descriptor_read_error, 0, _location} -> true
             _frame -> false
           end)

    assert log =~ "Hold state descriptor close failed after read raised"
    assert log =~ "simulated_close_failure"
  end

  test "load surfaces an unreadable state path" do
    workspace_root = workspace_root("unreadable")
    state_path = write_json_state!(workspace_root, %{"version" => 1, "holds" => []})
    File.chmod!(workspace_root, 0o000)

    on_exit(fn -> File.chmod(workspace_root, 0o700) end)

    assert {:error, {:hold_state_unreadable, ^state_path, reason}} = HoldStore.load(workspace_root)
    assert reason in [:eacces, :eperm]
  end

  test "load and persist reject an unavailable workspace root" do
    workspace_root = workspace_root("unavailable")
    File.mkdir_p!(Path.dirname(workspace_root))
    File.write!(workspace_root, "not a directory")
    expanded_root = Path.expand(workspace_root)

    assert {:error, {:hold_state_root_unavailable, ^expanded_root, reason}} =
             HoldStore.load(workspace_root)

    assert reason in [:eexist, :enotdir]

    assert {:error, {:hold_state_root_unavailable, ^expanded_root, persist_reason}} =
             HoldStore.persist(workspace_root, %{})

    assert persist_reason in [:eexist, :enotdir]
  end

  test "persist surfaces JSON encoding errors" do
    workspace_root = workspace_root("encode-error")
    invalid_hold = hold("issue-a", "SYM-1", datetime!("2026-07-21T12:00:00Z"), reason: self())

    assert {:error, {:hold_state_encode_failed, %Protocol.UndefinedError{}}} =
             HoldStore.persist(workspace_root, %{"issue-a" => invalid_hold})
  end

  test "persist surfaces an atomic rename failure and removes its temporary file" do
    workspace_root = workspace_root("rename-error")
    state_path = state_path(workspace_root)
    File.mkdir_p!(state_path)
    hold = hold("issue-a", "SYM-1", datetime!("2026-07-21T12:00:00Z"))

    assert {:error, {:hold_state_write_failed, ^state_path, reason}} =
             HoldStore.persist(workspace_root, %{"issue-a" => hold})

    assert reason in [:eisdir, :eexist, :enotempty, :eperm]
    assert Path.wildcard(state_path <> ".tmp.*") == []
  end

  test "persist surfaces an atomic temporary-file open failure" do
    workspace_root = workspace_root("open-error")
    state_path = state_path(workspace_root)
    File.mkdir_p!(workspace_root)
    File.chmod!(workspace_root, 0o500)

    on_exit(fn -> File.chmod(workspace_root, 0o700) end)

    assert {:error, {:hold_state_write_failed, ^state_path, reason}} =
             HoldStore.persist(workspace_root, %{})

    assert reason in [:eacces, :eperm]
  end

  test "atomic descriptor close failure prevents rename and removes the temporary file" do
    workspace_root = workspace_root("atomic-close-failure")
    state_path = state_path(workspace_root)
    File.mkdir_p!(workspace_root)
    test_pid = self()

    descriptor_closer = fn io_device ->
      :ok = File.close(io_device)
      {:error, :simulated_close_failure}
    end

    renamer = fn _temp_path, _state_path ->
      send(test_pid, :rename_attempted)
      :ok
    end

    expected =
      {:error, {:hold_state_write_failed, state_path, {:temporary_descriptor_close_failed, :simulated_close_failure}}}

    assert expected ==
             HoldStore.atomic_private_write_for_test(state_path, "payload",
               descriptor_closer: descriptor_closer,
               renamer: renamer
             )

    refute_received :rename_attempted
    refute File.exists?(state_path)
    assert Path.wildcard(state_path <> ".tmp.*") == []
  end

  test "atomic descriptor write and close errors are both preserved" do
    workspace_root = workspace_root("atomic-write-and-close-failure")
    state_path = state_path(workspace_root)
    File.mkdir_p!(workspace_root)

    descriptor_writer = fn _io_device, _temp_path, _encoded ->
      {:error, :simulated_write_failure}
    end

    descriptor_closer = fn io_device ->
      :ok = File.close(io_device)
      {:error, :simulated_close_failure}
    end

    write_reason = :simulated_write_failure
    close_reason = :simulated_close_failure
    combined_reason = {:temporary_descriptor_write_and_close_failed, write_reason, close_reason}
    expected = {:error, {:hold_state_write_failed, state_path, combined_reason}}

    assert expected ==
             HoldStore.atomic_private_write_for_test(state_path, "payload",
               descriptor_writer: descriptor_writer,
               descriptor_closer: descriptor_closer
             )

    refute File.exists?(state_path)
    assert Path.wildcard(state_path <> ".tmp.*") == []
  end

  defp workspace_root(suffix) do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-hold-store-#{suffix}-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(workspace_root) end)
    workspace_root
  end

  defp state_path(workspace_root), do: Path.join(workspace_root, @state_filename)

  defp write_json_state!(workspace_root, payload) do
    write_state!(workspace_root, Jason.encode!(payload))
  end

  defp write_state!(workspace_root, encoded), do: write_state!(workspace_root, @state_filename, encoded)

  defp write_state!(workspace_root, filename, encoded) do
    state_path = Path.join(workspace_root, filename)
    File.mkdir_p!(workspace_root)
    File.write!(state_path, encoded)
    File.chmod!(state_path, 0o600)
    state_path
  end

  defp encoded_hold do
    %{
      "issue_id" => "issue-a",
      "identifier" => "SYM-1",
      "reason" => "input_token_limit",
      "limit" => 1_000,
      "observed_tokens" => 1_001,
      "issue_state" => "In Progress",
      "worker_host" => "worker.example",
      "workspace_path" => "/tmp/SYM-1",
      "codex_app_server_pid" => nil,
      "cleanup_pending" => false,
      "held_at" => "2026-07-21T12:00:00.000000Z"
    }
  end

  defp hold(issue_id, identifier, held_at, overrides \\ []) do
    Enum.into(overrides, %{
      issue_id: issue_id,
      identifier: identifier,
      reason: "input_token_limit",
      limit: 1_000,
      observed_tokens: 1_001,
      issue_state: "In Progress",
      worker_host: "worker.example",
      workspace_path: "/tmp/#{identifier}",
      codex_app_server_pid: nil,
      cleanup_pending: false,
      held_at: held_at
    })
  end

  defp datetime!(encoded) do
    {:ok, datetime, 0} = DateTime.from_iso8601(encoded)
    datetime
  end

  defp raise_descriptor_read_error, do: raise("simulated descriptor read failure")
end
