defmodule SymphonyElixir.ControlTokenTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SymphonyElixir.ControlToken

  @fd_env "SYMPHONY_CONTROL_TOKEN_FD"
  @legacy_env "SYMPHONY_CONTROL_TOKEN"

  setup do
    Process.flag(:trap_exit, true)
    fd_env = System.get_env(@fd_env)
    legacy_env = System.get_env(@legacy_env)
    stored_token = ControlToken.fetch()

    on_exit(fn ->
      restore_env(@fd_env, fd_env)
      restore_env(@legacy_env, legacy_env)
      restore_stored_token(stored_token)
    end)

    System.delete_env(@fd_env)
    System.delete_env(@legacy_env)
    :ok
  end

  test "an absent descriptor leaves control unconfigured and ignores the legacy secret environment" do
    System.put_env(@legacy_env, "legacy-secret-must-not-be-used")

    assert {:ok, pid} = ControlToken.start_link(name: nil)
    assert {:error, :control_token_not_configured} = ControlToken.fetch(pid)
    refute System.get_env(@legacy_env)
    GenServer.stop(pid)
  end

  test "token validation rejects unsafe values" do
    for {token, reason} <- [
          {"", :empty_token},
          {"contains\nnewline", :invalid_token_bytes},
          {"contains\rreturn", :invalid_token_bytes},
          {"contains\0nul", :invalid_token_bytes},
          {" leading-space", :invalid_token_bytes},
          {"trailing-space ", :invalid_token_bytes},
          {"contains\ttab", :invalid_token_bytes},
          {<<0xFF>>, :invalid_token_bytes},
          {String.duplicate("x", 4_097), :token_too_large}
        ] do
      assert {:error, {:invalid_control_token_fd, ^reason}} =
               ControlToken.start_link(name: nil, source: {:token, token})
    end
  end

  test "reads one inherited anonymous pipe, closes it, and keeps the token out of argv and environment" do
    token = "owner-control-#{System.unique_integer([:positive])}"

    expression = """
    {:ok, pid} = SymphonyElixir.ControlToken.start_link(name: nil)
    {:ok, token} = SymphonyElixir.ControlToken.fetch(pid)
    env_exposed = Enum.any?(System.get_env(), fn {_key, value} -> String.contains?(value, token) end)
    argv_exposed = String.contains?(inspect(:init.get_arguments()), token)
    fd_closed = match?({:error, _}, :prim_file.file_desc_to_ref(9, [:read, :binary]))
    IO.puts("TOKEN_BYTES=\#{byte_size(token)}")
    IO.puts("ENV_EXPOSED=\#{env_exposed}")
    IO.puts("ARGV_EXPOSED=\#{argv_exposed}")
    IO.puts("FD_CLOSED=\#{fd_closed}")
    """

    {output, 0} = run_with_pipe(token, expression)

    assert output =~ "TOKEN_BYTES=#{byte_size(token)}"
    assert output =~ "ENV_EXPOSED=false"
    assert output =~ "ARGV_EXPOSED=false"
    assert output =~ "FD_CLOSED=true"
    refute output =~ token
  end

  test "the prepared child survives restart without retaining source metadata" do
    name = :control_token_restart_test
    token = "restart-stable-token"

    assert {:ok, child_spec} =
             ControlToken.child_spec_from_token_for_test(token, name: name, id: name)

    refute inspect(child_spec) =~ token
    assert {:ok, supervisor} = Supervisor.start_link([child_spec], strategy: :one_for_one)

    first_pid = Process.whereis(name)
    assert {:ok, ^token} = ControlToken.fetch(name)
    monitor = Process.monitor(first_pid)
    Process.exit(first_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^first_pid, :killed}

    second_pid = await_restarted_child(name, first_pid)
    assert {:ok, ^token} = ControlToken.fetch(name)
    assert second_pid != first_pid
    Supervisor.stop(supervisor)
  end

  test "a crash report never includes the stored token or restart metadata" do
    name = :control_token_crash_report_test
    token = "crash-report-secret-token"

    assert {:ok, child_spec} =
             ControlToken.child_spec_from_token_for_test(token, name: name, id: name)

    refute inspect(child_spec) =~ token
    assert {:ok, supervisor} = Supervisor.start_link([child_spec], strategy: :one_for_one)
    first_pid = Process.whereis(name)
    monitor = Process.monitor(first_pid)

    log =
      capture_log(fn ->
        caller =
          Task.async(fn ->
            catch_exit(GenServer.call(name, :unexpected_control_token_call))
          end)

        Task.await(caller)
        assert_receive {:DOWN, ^monitor, :process, ^first_pid, _reason}
        await_restarted_child(name, first_pid)
      end)

    assert log =~ "terminating"
    refute log =~ token
    assert {:ok, ^token} = ControlToken.fetch(name)
    Supervisor.stop(supervisor)
  end

  test "preparing the child rejects and consumes an invalid descriptor setting" do
    System.put_env(@fd_env, "not-a-descriptor")

    assert {:error, :invalid_fd_number} = ControlToken.child_spec_from_environment()
    refute System.get_env(@fd_env)
  end

  test "a configured regular-file descriptor fails closed and is closed" do
    path = Path.join(System.tmp_dir!(), "symphony-control-token-#{System.unique_integer([:positive])}")
    File.write!(path, "not-an-anonymous-pipe")

    try do
      {:ok, file_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      System.put_env(@fd_env, Integer.to_string(descriptor_number(file_ref)))

      assert {:error, {:invalid_control_token_fd, :not_a_pipe}} =
               ControlToken.start_link(name: nil)

      assert {:error, :ebadf} = :prim_file.read(file_ref, 1)
    after
      File.rm(path)
    end
  end

  test "an oversized inherited anonymous token fails closed and closes the pipe" do
    expression = """
    Process.flag(:trap_exit, true)
    result = SymphonyElixir.ControlToken.start_link(name: nil)
    fd_closed = match?({:error, _}, :prim_file.file_desc_to_ref(9, [:read, :binary]))
    IO.puts("RESULT=\#{inspect(result)}")
    IO.puts("FD_CLOSED=\#{fd_closed}")
    """

    {output, 0} = run_with_pipe(String.duplicate("x", 4_097), expression)

    assert output =~ "invalid_control_token_fd"
    assert output =~ "token_too_large"
    assert output =~ "FD_CLOSED=true"

    port = output_port(String.duplicate("x", 4_097))
    assert {:error, :token_too_large} = ControlToken.read_private_port_for_test(port, 1_000)
  end

  test "an inherited anonymous pipe that never reaches EOF times out and closes" do
    expression = """
    Process.flag(:trap_exit, true)
    started_at = System.monotonic_time(:millisecond)
    result = SymphonyElixir.ControlToken.start_link(name: nil)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    fd_closed = match?({:error, _}, :prim_file.file_desc_to_ref(9, [:read, :binary]))
    IO.puts("RESULT=\#{inspect(result)}")
    IO.puts("ELAPSED_MS=\#{elapsed_ms}")
    IO.puts("FD_CLOSED=\#{fd_closed}")
    """

    {output, 0} = run_with_open_pipe(expression)
    [elapsed_ms] = Regex.run(~r/ELAPSED_MS=(\d+)/, output, capture: :all_but_first)

    assert output =~ "invalid_control_token_fd"
    assert output =~ "read_timeout"
    assert output =~ "FD_CLOSED=true"
    assert String.to_integer(elapsed_ms) in 2_000..4_000
  end

  test "a named FIFO is rejected without reopening or blocking" do
    with_fifo_contents("named-fifo-token", fn fd, file_ref ->
      System.put_env(@fd_env, Integer.to_string(fd))
      started_at = System.monotonic_time(:millisecond)

      assert {:error, {:invalid_control_token_fd, :not_anonymous_pipe}} =
               ControlToken.start_link(name: nil)

      assert System.monotonic_time(:millisecond) - started_at < 500
      assert {:error, :ebadf} = :prim_file.read(file_ref, 1)
    end)
  end

  test "an unlinked named FIFO cannot impersonate an anonymous pipe" do
    with_fifo(fn path ->
      writer =
        Task.async(fn ->
          {:ok, writer_ref} = :prim_file.open(String.to_charlist(path), [:write, :binary])
          :ok = :prim_file.write(writer_ref, "unlinked-named-fifo-token")
          :ok = :prim_file.close(writer_ref)
        end)

      {:ok, reader_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      :ok = Task.await(writer)
      :ok = File.rm(path)
      System.put_env(@fd_env, Integer.to_string(descriptor_number(reader_ref)))
      started_at = System.monotonic_time(:millisecond)

      assert {:error, {:invalid_control_token_fd, :not_anonymous_pipe}} =
               ControlToken.start_link(name: nil)

      assert System.monotonic_time(:millisecond) - started_at < 500
      assert {:error, :ebadf} = :prim_file.read(reader_ref, 1)
    end)
  end

  test "reads a prepared token into the owner store" do
    assert {:ok, pid} = ControlToken.start_link(name: nil, source: {:token, "owner-token"})
    assert {:ok, "owner-token"} = ControlToken.fetch(pid)
    GenServer.stop(pid)
  end

  test "reads and validates data from a native input port" do
    assert {:ok, "owner-port-token"} =
             "owner-port-token"
             |> output_port()
             |> ControlToken.read_private_port_for_test(1_000)

    path = Path.join(System.tmp_dir!(), "symphony-adopted-fd-#{System.os_time(:nanosecond)}")
    File.write!(path, "test-only-adopted-fd")

    try do
      {:ok, source_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      fd = descriptor_number(source_ref)

      assert {:ok, "injected-owner-port-token"} =
               ControlToken.read_adopted_fd_for_test(
                 fd,
                 source_ref,
                 1_000,
                 fn ^source_ref, ^fd -> :ok end,
                 fn ^fd -> {:ok, output_port("injected-owner-port-token")} end
               )

      assert {:error, _reason} = :prim_file.read(source_ref, 1)
    after
      File.rm(path)
    end
  end

  test "an invalid configured descriptor fails closed" do
    System.put_env(@fd_env, "not-a-descriptor")

    assert {:error, {:invalid_control_token_fd, :invalid_fd_number}} =
             ControlToken.start_link(name: nil)
  end

  test "a closed configured descriptor fails closed" do
    path = Path.join(System.tmp_dir!(), "symphony-control-token-closed-#{System.os_time(:nanosecond)}")
    File.write!(path, "already-closed")

    try do
      {:ok, file_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      fd = descriptor_number(file_ref)
      :ok = :prim_file.close(file_ref)
      assert {:error, :invalid_fd} = ControlToken.require_anonymous_pipe_for_test(file_ref, fd)
      System.put_env(@fd_env, Integer.to_string(fd))

      assert {:error, {:invalid_control_token_fd, :invalid_fd}} =
               ControlToken.start_link(name: nil)
    after
      File.rm(path)
    end
  end

  test "an unexpected native port exit fails closed" do
    executable = System.find_executable("true") || flunk("true executable not found")
    port = Port.open({:spawn_executable, executable}, [:binary, :exit_status])
    Process.unlink(port)

    assert {:error, :read_failed} = ControlToken.read_private_port_for_test(port, 1_000)
  end

  test "native descriptor port setup fails closed for an impossible descriptor" do
    assert {:error, :invalid_fd} = ControlToken.open_private_port_for_test(-1)

    path = Path.join(System.tmp_dir!(), "symphony-control-port-#{System.os_time(:nanosecond)}")
    File.write!(path, "test-only-port-input")

    try do
      {:ok, file_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      assert {:ok, port} = ControlToken.open_private_port_for_test(descriptor_number(file_ref))
      assert true = Port.close(port)
      assert :ok = :prim_file.close(file_ref)
    after
      File.rm(path)
    end
  end

  test "partial input cannot extend the absolute descriptor deadline" do
    executable = System.find_executable("sh") || flunk("sh executable not found")

    port =
      Port.open(
        {:spawn_executable, executable},
        [:binary, :eof, args: [~c"-c", ~c"for i in 1 2 3 4 5; do printf x; sleep .075; done"]]
      )

    Process.unlink(port)
    started_at = System.monotonic_time(:millisecond)

    assert {:error, :read_timeout} = ControlToken.read_private_port_for_test(port, 200)

    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms >= 200
    assert elapsed_ms < 350
  end

  test "pipe identity validation is explicit and fail closed on every supported platform" do
    assert :ok =
             ControlToken.validate_pipe_identity_for_test(
               {:unix, :linux},
               {:ok, "pipe:[12345]"},
               1
             )

    assert {:error, :not_anonymous_pipe} =
             ControlToken.validate_pipe_identity_for_test(
               {:unix, :linux},
               {:ok, "/tmp/named-fifo"},
               1
             )

    assert {:error, :not_anonymous_pipe} =
             ControlToken.validate_pipe_identity_for_test({:unix, :linux}, {:error, :enoent}, 0)

    assert :ok =
             ControlToken.validate_pipe_identity_for_test({:unix, :darwin}, :not_required, 0)

    assert {:error, :not_anonymous_pipe} =
             ControlToken.validate_pipe_identity_for_test({:unix, :darwin}, :not_required, 1)

    assert {:error, :unsupported_platform} =
             ControlToken.validate_pipe_identity_for_test({:win32, :nt}, :not_required, 0)

    assert match?(
             {status, _value} when status in [:ok, :error],
             ControlToken.descriptor_link_for_test({:unix, :linux}, 0)
           )

    assert :not_required = ControlToken.descriptor_link_for_test({:unix, :darwin}, 0)
  end

  test "the test owner replacement updates the named store" do
    assert {:ok, child_spec} = ControlToken.child_spec_from_token_for_test("default-child-token")
    assert child_spec.id == ControlToken

    assert :ok = ControlToken.replace_for_test("replacement-token")
    assert {:ok, "replacement-token"} = ControlToken.fetch()
  end

  test "the zero-argument start uses the owner name" do
    :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, ControlToken)

    try do
      assert {:ok, pid} = ControlToken.start_link()
      assert Process.whereis(ControlToken) == pid
      GenServer.stop(pid)
    after
      case Supervisor.restart_child(SymphonyElixir.Supervisor, ControlToken) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end

  test "declares both the legacy secret and descriptor name as child-scrubbed" do
    assert ControlToken.secret_environment_names() == [@legacy_env, @fd_env]
  end

  defp run_with_pipe(token, expression) do
    script = """
    printf %s "$CONTROL_TOKEN_FIXTURE" |
    (
      unset CONTROL_TOKEN_FIXTURE SYMPHONY_CONTROL_TOKEN
      exec 9<&0
      export SYMPHONY_CONTROL_TOKEN_FD=9
      exec "$1" -pa "$2" -e "$3"
    )
    """

    System.cmd(
      "sh",
      ["-c", script, "control-token-pipe", elixir_executable(), ebin_path(), expression],
      env: [{"CONTROL_TOKEN_FIXTURE", token}],
      stderr_to_stdout: true
    )
  end

  defp run_with_open_pipe(expression) do
    script = """
    sleep 3 |
    (
      unset SYMPHONY_CONTROL_TOKEN
      exec 9<&0
      export SYMPHONY_CONTROL_TOKEN_FD=9
      exec "$1" -pa "$2" -e "$3"
    )
    """

    System.cmd(
      "sh",
      ["-c", script, "control-token-open-pipe", elixir_executable(), ebin_path(), expression],
      stderr_to_stdout: true
    )
  end

  defp output_port(contents) do
    executable = System.find_executable("printf") || flunk("printf executable not found")
    port = Port.open({:spawn_executable, executable}, [:binary, :eof, args: [contents]])
    Process.unlink(port)
    port
  end

  defp with_fifo_contents(contents, assertion) do
    with_fifo(fn path ->
      writer =
        Task.async(fn ->
          {:ok, writer_ref} = :prim_file.open(String.to_charlist(path), [:write, :binary])
          :ok = :prim_file.write(writer_ref, contents)
          :ok = :prim_file.close(writer_ref)
        end)

      {:ok, reader_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      :ok = Task.await(writer)
      assertion.(descriptor_number(reader_ref), reader_ref)
    end)
  end

  defp with_fifo(assertion) do
    path = Path.join(System.tmp_dir!(), "symphony-control-token-fifo-#{System.os_time(:nanosecond)}")
    {_, 0} = System.cmd("mkfifo", [path])

    try do
      assertion.(path)
    after
      File.rm(path)
    end
  end

  defp descriptor_number(file_ref), do: file_ref |> :prim_file.get_handle() |> :binary.decode_unsigned(:little)

  defp await_restarted_child(name, previous_pid, attempts \\ 100)

  defp await_restarted_child(_name, _previous_pid, 0), do: flunk("control token child did not restart")

  defp await_restarted_child(name, previous_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != previous_pid ->
        pid

      _ ->
        Process.sleep(10)
        await_restarted_child(name, previous_pid, attempts - 1)
    end
  end

  defp elixir_executable do
    System.find_executable("elixir") || flunk("elixir executable not found")
  end

  defp ebin_path do
    Path.expand("_build/test/lib/symphony_elixir/ebin")
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)

  defp restore_stored_token({:ok, token}), do: ControlToken.replace_for_test(token)

  defp restore_stored_token({:error, :control_token_not_configured}),
    do: ControlToken.replace_for_test(nil)
end
