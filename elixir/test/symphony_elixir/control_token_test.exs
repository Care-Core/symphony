defmodule SymphonyElixir.ControlTokenTest do
  use ExUnit.Case, async: false

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
    GenServer.stop(pid)
  end

  test "token validation rejects unsafe values" do
    for {token, reason} <- [
          {"", :empty_token},
          {"contains\nnewline", :invalid_token_bytes},
          {"contains\rreturn", :invalid_token_bytes},
          {"contains\0nul", :invalid_token_bytes},
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

  test "an oversized inherited token fails closed and closes the pipe" do
    with_fifo_contents(String.duplicate("x", 4_097), fn fd, file_ref ->
      System.put_env(@fd_env, Integer.to_string(fd))

      assert {:error, {:invalid_control_token_fd, :token_too_large}} =
               ControlToken.start_link(name: nil)

      assert {:error, :ebadf} = :prim_file.read(file_ref, 1)
    end)
  end

  test "an inherited pipe that never reaches EOF times out and closes" do
    path = Path.join(System.tmp_dir!(), "symphony-control-token-timeout-#{System.os_time(:nanosecond)}")
    File.write!(path, "close-me-after-timeout")

    try do
      {:ok, file_ref} = :prim_file.open(String.to_charlist(path), [:read, :binary])
      fd = descriptor_number(file_ref)
      reader = fn _fd -> Process.sleep(60_000) end

      assert {:error, :read_timeout} = ControlToken.read_fd_for_test(fd, reader, 10)
      assert {:error, :ebadf} = :prim_file.read(file_ref, 1)
    after
      File.rm(path)
    end
  end

  test "reads a configured pipe into the owner store" do
    with_fifo_contents("owner-token", fn fd, file_ref ->
      System.put_env(@fd_env, Integer.to_string(fd))

      assert {:ok, pid} = ControlToken.start_link(name: nil)
      assert {:ok, "owner-token"} = ControlToken.fetch(pid)
      assert {:error, :ebadf} = :prim_file.read(file_ref, 1)
      GenServer.stop(pid)
    end)
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
      System.put_env(@fd_env, Integer.to_string(fd))

      assert {:error, {:invalid_control_token_fd, :invalid_fd}} =
               ControlToken.start_link(name: nil)
    after
      File.rm(path)
    end
  end

  test "an unexpected reader exit fails closed" do
    assert {:error, :read_failed} =
             ControlToken.read_fd_for_test(1_000_000, fn _fd -> exit(:fixture_crash) end, 50)
  end

  test "a descriptor read error fails closed" do
    assert {:error, :read_failed} =
             ControlToken.read_bounded_for_test(fn _file_ref, _read_size -> {:error, :eio} end)
  end

  test "the test owner replacement updates the named store" do
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
