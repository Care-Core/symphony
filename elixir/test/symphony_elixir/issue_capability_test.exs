defmodule SymphonyElixir.IssueCapabilityTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.IssueCapability

  @fd_env "SYMPHONY_ISSUE_CAPABILITY_KEY_FD"
  @restart_env "SYMPHONY_RESTART_ID"
  @restart_id "123e4567-e89b-42d3-a456-426614174000"
  @nonce "123e4567-e89b-42d3-a456-426614174001"
  @issue "CC-1898"
  @session "thread-turn"
  @hash String.duplicate("a", 64)
  @head String.duplicate("b", 40)

  setup do
    fd_env = System.get_env(@fd_env)
    restart_env = System.get_env(@restart_env)
    stored = IssueCapability.fetch()
    :ok = IssueCapability.replace_for_test(nil, nil)

    on_exit(fn ->
      restore_env(@fd_env, fd_env)
      restore_env(@restart_env, restart_env)
      restore_configuration(stored)
    end)

    System.delete_env(@fd_env)
    System.delete_env(@restart_env)
    :ok
  end

  test "an absent private configuration remains disabled and produces a restart-stable child" do
    assert {:ok, child_spec} =
             IssueCapability.child_spec_from_environment(
               id: :disabled_issue_capability,
               name: :disabled_issue_capability
             )

    assert child_spec.id == :disabled_issue_capability
    assert {:error, :issue_capability_not_configured} = IssueCapability.fetch()
    assert {:ok, supervisor} = Supervisor.start_link([child_spec], strategy: :one_for_one)
    assert {:error, :issue_capability_not_configured} = IssueCapability.fetch()
    Supervisor.stop(supervisor)
  end

  test "an unnamed prepared store can be started directly" do
    assert {:ok, pid} = IssueCapability.start_link(name: nil)
    assert {:error, :issue_capability_not_configured} = IssueCapability.fetch(pid)
    GenServer.stop(pid)
  end

  test "the zero-argument start uses the owner name" do
    :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, IssueCapability)

    try do
      assert {:ok, pid} = IssueCapability.start_link()
      assert Process.whereis(IssueCapability) == pid
      GenServer.stop(pid)
    after
      case Supervisor.restart_child(SymphonyElixir.Supervisor, IssueCapability) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end

  test "environment preparation consumes only the private descriptor and rejects partial configuration" do
    System.put_env(@fd_env, "not-a-descriptor")
    System.put_env(@restart_env, @restart_id)

    assert {:error, :invalid_fd_number} = IssueCapability.child_spec_from_environment()
    refute System.get_env(@fd_env)
    assert System.get_env(@restart_env) == @restart_id

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.child_spec_from_environment_for_test(
               "9",
               nil,
               fn "9", 32 ->
                 send(self(), :partial_descriptor_consumed)
                 {:ok, key()}
               end
             )

    assert_received :partial_descriptor_consumed

    System.put_env(@restart_env, @restart_id)

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.child_spec_from_environment()

    assert System.get_env(@restart_env) == @restart_id
  end

  test "a same-restart application start reuses the consumed private configuration" do
    secret = key()
    :ok = IssueCapability.replace_for_test(secret, @restart_id)

    assert {:ok, child_spec} =
             IssueCapability.child_spec_from_environment_for_test(
               nil,
               @restart_id,
               fn _, _ -> flunk("same-restart reuse must not reread a descriptor") end
             )

    assert child_spec.id == IssueCapability

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.child_spec_from_environment_for_test(
               nil,
               "123e4567-e89b-42d3-a456-426614174099",
               fn _, _ -> flunk("mismatched restart must not read a descriptor") end
             )
  end

  test "configuration validation requires exactly 32 bytes and a canonical restart id" do
    assert {:error, :invalid_issue_capability_key_size} =
             IssueCapability.child_spec_from_key_for_test(<<1>>, @restart_id)

    assert {:error, :invalid_issue_capability_restart_id} =
             IssueCapability.child_spec_from_key_for_test(key(), "not-a-restart")

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.child_spec_from_key_for_test(nil, @restart_id)

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.replace_for_test(:not_binary, @restart_id)
  end

  test "environment preparation validates the descriptor reader result" do
    secret = key()

    assert {:ok, child_spec} =
             IssueCapability.child_spec_from_environment_for_test(
               "10",
               @restart_id,
               fn "10", 32 -> {:ok, secret} end,
               id: :reader_issue_capability,
               name: :reader_issue_capability
             )

    assert child_spec.id == :reader_issue_capability
    assert {:ok, %{key: ^secret, restart_id: @restart_id}} = IssueCapability.fetch()

    assert {:error, :read_failed} =
             IssueCapability.child_spec_from_environment_for_test(
               "10",
               @restart_id,
               fn "10", 32 -> {:error, :read_failed} end
             )

    assert {:error, :incomplete_issue_capability_configuration} =
             IssueCapability.child_spec_from_environment_for_test(
               :not_a_descriptor,
               @restart_id,
               fn _, _ -> flunk("invalid descriptor types must not be read") end
             )
  end

  test "a prepared secret survives child restart without entering the child specification" do
    name = :issue_capability_restart_test
    secret = key()

    assert {:ok, child_spec} =
             IssueCapability.child_spec_from_key_for_test(secret, @restart_id,
               name: name,
               id: name
             )

    refute inspect(child_spec) =~ Base.encode16(secret)
    assert {:ok, supervisor} = Supervisor.start_link([child_spec], strategy: :one_for_one)
    assert {:ok, %{key: ^secret, restart_id: @restart_id}} = IssueCapability.fetch(name)

    first_pid = Process.whereis(name)
    monitor = Process.monitor(first_pid)
    Process.exit(first_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^first_pid, :killed}
    assert eventually(fn -> Process.whereis(name) not in [nil, first_pid] end)
    assert {:ok, %{key: ^secret, restart_id: @restart_id}} = IssueCapability.fetch(name)
    Supervisor.stop(supervisor)
  end

  test "reads the owner key from one anonymous pipe and retains the public restart identity" do
    expression = """
    {:ok, child_spec} = SymphonyElixir.IssueCapability.child_spec_from_environment()
    {:ok, supervisor} = Supervisor.start_link([child_spec], strategy: :one_for_one)
    {:ok, configuration} = SymphonyElixir.IssueCapability.fetch()
    fd_closed = match?({:error, _}, :prim_file.file_desc_to_ref(3, [:read, :binary]))
    IO.puts("KEY_BYTES=\#{byte_size(configuration.key)}")
    IO.puts("RESTART_RETAINED=\#{System.get_env(\"SYMPHONY_RESTART_ID\") == \"#{@restart_id}\"}")
    IO.puts("FD_CLOSED=\#{fd_closed}")
    Supervisor.stop(supervisor)
    """

    {output, 0} = run_with_pipe(:binary.copy("k", 32), expression)

    assert output =~ "KEY_BYTES=32"
    assert output =~ "RESTART_RETAINED=true"
    assert output =~ "FD_CLOSED=true"
    refute output =~ :binary.copy("k", 32)
  end

  test "request signatures bind the exact body issue session restart and nonce" do
    secret = key()
    :ok = IssueCapability.replace_for_test(secret, @restart_id)
    payload = request_payload(@nonce, @session, @restart_id)
    signature = request_signature(secret, :progress, @nonce, @issue, @session, @restart_id, payload)

    assert :ok =
             IssueCapability.verify_request(
               :progress,
               signature,
               @issue,
               @session,
               @restart_id,
               @nonce,
               payload
             )

    assert :ok =
             IssueCapability.verify_request(
               :progress,
               signature,
               @issue,
               @session,
               @restart_id,
               @nonce,
               payload
             )

    for {candidate, issue, session, restart_id, candidate_payload} <- [
          {signature, "CC-1899", @session, @restart_id, payload},
          {signature, @issue, "successor", @restart_id, payload},
          {signature, @issue, @session, "123e4567-e89b-42d3-a456-426614174099", payload},
          {signature <> "extra", @issue, @session, @restart_id, payload},
          {nil, @issue, @session, @restart_id, payload},
          {signature, @issue, @session, @restart_id, Map.put(payload, "progress_kind", "tampered")}
        ] do
      assert {:error, :invalid_issue_capability} =
               IssueCapability.verify_request(
                 :progress,
                 candidate,
                 issue,
                 session,
                 restart_id,
                 "123e4567-e89b-42d3-a456-426614174099",
                 candidate_payload
               )
    end

    assert {:error, :invalid_issue_capability} =
             IssueCapability.verify_request(
               :progress,
               signature,
               "",
               @session,
               @restart_id,
               "123e4567-e89b-42d3-a456-426614174098",
               payload
             )

    assert {:error, :invalid_issue_capability} =
             IssueCapability.verify_request(
               :progress,
               signature,
               @issue,
               @session,
               @restart_id,
               "bad-nonce",
               payload
             )
  end

  test "request signatures keep issue and session identities unambiguous" do
    secret = key()
    :ok = IssueCapability.replace_for_test(secret, @restart_id)
    payload = request_payload(@nonce, "C", @restart_id)
    signed = request_signature(secret, :progress, @nonce, "A:B", "C", @restart_id, payload)

    assert :ok =
             IssueCapability.verify_request(
               :progress,
               signed,
               "A:B",
               "C",
               @restart_id,
               @nonce,
               payload
             )

    assert {:error, :invalid_issue_capability} =
             IssueCapability.verify_request(
               :progress,
               signed,
               "A",
               "B:C",
               @restart_id,
               "123e4567-e89b-42d3-a456-426614174099",
               payload
             )
  end

  test "request signatures match the CareCore broker wire fixture" do
    :ok = IssueCapability.replace_for_test(key(), @restart_id)

    payload = %{
      "capability_nonce" => @nonce,
      "fingerprint" => %{
        "contract_revision" => "v1",
        "required_check_set" => ["Test & Lint"]
      },
      "owner_session" => "session-1",
      "progress_kind" => "workpad_checkpoint",
      "progress_receipt" => "receipt-1",
      "restart_id" => @restart_id
    }

    assert :ok =
             IssueCapability.verify_request(
               :progress,
               "T17JTdqiD6nR4WsnP3W8JtH1SbHuH0BWxzrCLanYnDU",
               "CC-123",
               "session-1",
               @restart_id,
               @nonce,
               payload
             )
  end

  test "request verification rejects unsupported actions" do
    :ok = IssueCapability.replace_for_test(key(), @restart_id)
    successor_nonce = "123e4567-e89b-42d3-a456-426614174099"
    successor_payload = request_payload(successor_nonce, @session, @restart_id)

    successor_signature =
      request_signature(
        key(),
        :progress,
        successor_nonce,
        @issue,
        @session,
        @restart_id,
        successor_payload
      )

    assert {:error, :invalid_issue_capability} =
             IssueCapability.verify_request(
               :unsupported,
               successor_signature,
               @issue,
               @session,
               @restart_id,
               successor_nonce,
               successor_payload
             )
  end

  test "verification and signing fail closed while unconfigured" do
    assert {:error, :issue_capability_not_configured} =
             IssueCapability.verify_request(
               :progress,
               "anything",
               @issue,
               @session,
               @restart_id,
               @nonce,
               request_payload(@nonce, @session, @restart_id)
             )

    assert {:error, :issue_capability_not_configured} =
             IssueCapability.sign_response(
               :progress,
               @nonce,
               @issue,
               @session,
               progress_payload()
             )
  end

  test "progress responses are nonce-bound and tamper evident" do
    :ok = IssueCapability.replace_for_test(key(), @restart_id)
    payload = progress_payload()

    assert {:ok, signature} =
             IssueCapability.sign_response(:progress, @nonce, @issue, @session, payload)

    assert IssueCapability.verify_response_for_test(
             :progress,
             @nonce,
             @issue,
             @session,
             payload,
             signature
           )

    refute IssueCapability.verify_response_for_test(
             :progress,
             "123e4567-e89b-42d3-a456-426614174099",
             @issue,
             @session,
             payload,
             signature
           )

    refute IssueCapability.verify_response_for_test(
             :progress,
             @nonce,
             @issue,
             @session,
             %{payload | changed: false},
             signature
           )

    refute IssueCapability.verify_response_for_test(
             :progress,
             @nonce,
             @issue,
             @session,
             payload,
             nil
           )
  end

  test "review responses use the same authenticated channel" do
    :ok = IssueCapability.replace_for_test(key(), @restart_id)
    payload = review_payload()

    assert {:ok, signature} =
             IssueCapability.sign_response(:review, @nonce, @issue, @session, payload)

    assert IssueCapability.verify_response_for_test(
             :review,
             @nonce,
             @issue,
             @session,
             payload,
             signature
           )

    for invalid_payload <- [
          %{payload | authorized: false},
          %{payload | kind: "other"},
          %{payload | review_round_count: -1},
          %{payload | security_review_count: -1},
          %{payload | authorization: "short"},
          %{payload | requested_head: "short"},
          %{}
        ] do
      assert {:error, :invalid_issue_capability_response} =
               IssueCapability.sign_response(
                 :review,
                 @nonce,
                 @issue,
                 @session,
                 invalid_payload
               )
    end
  end

  test "response signing rejects unsupported kinds identities nonces and payloads" do
    :ok = IssueCapability.replace_for_test(key(), @restart_id)

    for arguments <- [
          [:unsupported, @nonce, @issue, @session, progress_payload()],
          [:progress, "bad-nonce", @issue, @session, progress_payload()],
          [:progress, nil, @issue, @session, progress_payload()],
          [:progress, @nonce, "", @session, progress_payload()],
          [:progress, @nonce, @issue, @session, %{changed: true}],
          [:progress, @nonce, @issue, @session, :not_a_map]
        ] do
      assert {:error, :invalid_issue_capability_response} =
               apply(IssueCapability, :sign_response, arguments)
    end
  end

  test "invalid runtime sources stop without retaining secret input" do
    assert {:stop, :invalid_issue_capability_source} =
             IssueCapability.init(source: :environment)
  end

  test "declares the private descriptor as child-scrubbed" do
    assert IssueCapability.secret_environment_names() == [@fd_env]
  end

  defp key, do: :binary.copy(<<7>>, 32)

  defp request_payload(nonce, session, restart_id) do
    %{
      "capability_nonce" => nonce,
      "fingerprint" => %{"head_sha" => @head, "required_check_set" => ["test"]},
      "owner_session" => session,
      "progress_kind" => "validation_receipt",
      "progress_receipt" => "receipt-1",
      "restart_id" => restart_id
    }
  end

  defp request_signature(secret, kind, nonce, issue, session, restart_id, payload) do
    message =
      Jason.encode!([
        1,
        "request",
        Atom.to_string(kind),
        nonce,
        issue,
        session,
        restart_id,
        canonical_term(payload)
      ])

    :crypto.mac(:hmac, :sha256, secret, message)
    |> Base.url_encode64(padding: false)
  end

  defp canonical_term(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> [to_string(key), canonical_term(nested)] end)
    |> Enum.sort()
  end

  defp canonical_term(value) when is_list(value), do: Enum.map(value, &canonical_term/1)
  defp canonical_term(value), do: value

  defp progress_payload do
    %{
      changed: true,
      progress_fingerprint: @hash,
      review_fingerprint: String.duplicate("c", 64)
    }
  end

  defp review_payload do
    %{
      authorized: true,
      authorization: @hash,
      kind: "full",
      review_round_count: 1,
      security_review_count: 0,
      review_fingerprint: String.duplicate("c", 64),
      requested_head: @head
    }
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false

  defp run_with_pipe(secret, expression) do
    script = """
    printf %s "$ISSUE_CAPABILITY_KEY_FIXTURE" |
    (
      unset ISSUE_CAPABILITY_KEY_FIXTURE
      exec 3<&0
      export SYMPHONY_ISSUE_CAPABILITY_KEY_FD=3
      export SYMPHONY_RESTART_ID="$4"
      exec "$1" -pa "$2" -e "$3"
    )
    """

    System.cmd(
      "sh",
      ["-c", script, "issue-capability-pipe", elixir_executable(), ebin_path(), expression, @restart_id],
      env: [{"ISSUE_CAPABILITY_KEY_FIXTURE", secret}],
      stderr_to_stdout: true
    )
  end

  defp elixir_executable do
    System.find_executable("elixir") || flunk("elixir executable not found")
  end

  defp ebin_path, do: Path.expand("_build/test/lib/symphony_elixir/ebin")

  defp restore_configuration({:ok, %{key: key, restart_id: restart_id}}),
    do: IssueCapability.replace_for_test(key, restart_id)

  defp restore_configuration({:error, :issue_capability_not_configured}),
    do: IssueCapability.replace_for_test(nil, nil)

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
