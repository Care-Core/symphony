defmodule SymphonyElixir.RunnerCapabilitiesTest do
  use SymphonyElixir.TestSupport

  import Bitwise

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.RunnerCapabilities

  @runtime_env_names [
    "CODEX_HOME",
    "SYMPHONY_CODEX_BIN",
    "SYMPHONY_REAL_CODEX_BIN",
    "SYMPHONY_REVIEW_CODEX_HOME",
    "SYMPHONY_BROWSER_BACKEND_URL"
  ]

  setup do
    previous_env = Map.new(@runtime_env_names, &{&1, System.get_env(&1)})

    on_exit(fn ->
      Enum.each(previous_env, fn {name, value} -> restore_env(name, value) end)
    end)

    :ok
  end

  test "preflight normalizes a legacy Codex binary and prepares isolated reviewer state" do
    harness = capability_harness()
    System.put_env("SYMPHONY_CODEX_BIN", harness.real_codex)
    System.delete_env("SYMPHONY_REAL_CODEX_BIN")
    test_pid = self()

    assert :ok =
             RunnerCapabilities.preflight(
               settings: harness.settings,
               wrapper_path: harness.wrapper,
               review_probe: fn context ->
                 send(test_pid, {:review_probe, context})
                 :ok
               end,
               browser_probe: fn context ->
                 send(test_pid, {:browser_probe, context})
                 :ok
               end
             )

    assert_receive {:review_probe, context}
    assert_receive {:browser_probe, ^context}
    assert context.source_repo == harness.source_repo
    assert context.real_codex_bin == harness.real_codex
    assert context.wrapper_codex_bin == harness.wrapper
    assert context.reviewer_codex_home == harness.reviewer_home
    assert System.get_env("SYMPHONY_CODEX_BIN") == harness.wrapper
    assert System.get_env("SYMPHONY_REAL_CODEX_BIN") == harness.real_codex
    assert System.get_env("SYMPHONY_REVIEW_CODEX_HOME") == harness.reviewer_home
    assert System.get_env("SYMPHONY_BROWSER_BACKEND_URL") == "http://127.0.0.1:9377"
    assert System.get_env("CODEX_HOME") == harness.primary_home

    reviewer_auth = Path.join(harness.reviewer_home, "auth.json")
    assert File.read!(reviewer_auth) == "test-auth\n"
    assert file_mode(reviewer_auth) == 0o600
    assert file_mode(harness.reviewer_home) == 0o700
    assert file_mode(Path.join(harness.reviewer_home, "tmp")) == 0o700

    reviewer_skill = Path.join([harness.reviewer_home, "skills", "security-best-practices", "SKILL.md"])
    assert File.read!(reviewer_skill) == "---\nname: security-best-practices\n---\n"
    assert file_mode(reviewer_skill) == 0o600
    assert Path.wildcard(Path.join(harness.reviewer_home, ".auth.json.*")) == []
  end

  test "reviewer home must be absolute and beneath a private canonical parent" do
    harness = capability_harness()
    relative_runner = %{harness.settings.runner | reviewer_codex_home: "relative-reviewer-home"}
    relative_settings = %{harness.settings | runner: relative_runner}

    assert {:error, :reviewer_codex_home_must_be_absolute} =
             RunnerCapabilities.prepare_context(relative_settings, wrapper_path: harness.wrapper)

    File.chmod!(Path.dirname(harness.reviewer_home), 0o755)

    assert {:error, {:unexpected_mode, _path, 0o755, 0o700}} =
             RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)
  end

  test "reviewer home must remain isolated from the primary Codex home" do
    harness = capability_harness()
    runner = %{harness.settings.runner | reviewer_codex_home: harness.primary_home}
    settings = %{harness.settings | runner: runner}

    assert {:error, {:reviewer_codex_home_not_isolated, reviewer_home}} =
             RunnerCapabilities.prepare_context(settings, wrapper_path: harness.wrapper)

    assert reviewer_home == harness.primary_home
    assert File.read!(Path.join(harness.primary_home, "auth.json")) == "test-auth\n"
  end

  test "preflight requires the app-server command to preserve its minimal wrapper environment" do
    harness = capability_harness()
    settings = %{harness.settings | codex: %{harness.settings.codex | command: "codex app-server"}}

    assert {:error, :codex_command_must_disable_shell_environment_inheritance} =
             RunnerCapabilities.prepare_context(settings, wrapper_path: harness.wrapper)
  end

  test "primary auth must be a private regular file" do
    harness = capability_harness()
    auth_path = Path.join(harness.primary_home, "auth.json")
    File.chmod!(auth_path, 0o644)

    assert {:error, {:private_file_mode, ^auth_path, 0o644}} =
             RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)

    File.rm!(auth_path)
    target = Path.join(harness.root, "primary-auth-target")
    write_private_file(target, "unchanged\n")
    File.ln_s!(target, auth_path)

    assert {:error, {:private_file_symlink, ^auth_path}} =
             RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)

    assert File.read!(target) == "unchanged\n"
  end

  test "pre-placed reviewer auth symlinks are rejected without touching their target" do
    harness = capability_harness(create_reviewer_home: true)
    reviewer_auth = Path.join(harness.reviewer_home, "auth.json")
    target = Path.join(harness.root, "reviewer-auth-target")
    write_private_file(target, "unchanged\n")
    File.ln_s!(target, reviewer_auth)

    assert {:error, {:reviewer_auth_symlink, ^reviewer_auth}} =
             RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)

    assert File.read!(target) == "unchanged\n"
  end

  test "review canary uses the source repo, a minimal environment, and a bounded sandbox" do
    harness = capability_harness()
    System.put_env("SYMPHONY_REAL_CODEX_BIN", harness.real_codex)
    {:ok, context} = RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)
    test_pid = self()

    command_runner = fn executable, args, opts ->
      send(test_pid, {:review_command, executable, args, opts})
      {:ok, %{status: 1, output: "OpenAI Codex v1.2.3\nreview found an issue\n"}}
    end

    assert :ok = RunnerCapabilities.probe_review(context, command_runner: command_runner)
    assert_receive {:review_command, executable, args, opts}
    assert executable == harness.sandbox_codex
    assert opts[:cd] == harness.source_repo
    assert opts[:timeout_ms] == 20_000
    assert Enum.member?(args, "-i")
    assert Enum.any?(args, &String.contains?(&1, "network = {enabled = false}"))
    assert Enum.any?(args, &String.contains?(&1, harness.source_repo))
    assert Enum.any?(args, &String.starts_with?(&1, "HOME=#{harness.reviewer_home}"))
    assert Enum.member?(args, "SYMPHONY_CODEX_BIN=#{harness.wrapper}")
    assert Enum.member?(args, "SYMPHONY_BROWSER_BACKEND_URL=http://127.0.0.1:9377")
    refute Enum.any?(args, &String.contains?(&1, "LINEAR_API_KEY"))

    assert [shell, "-c", ~S|exec "$SYMPHONY_CODEX_BIN" "$@"|, "symphony-canary", "review", "--commit", "HEAD"] =
             Enum.take(args, -7)

    assert Path.basename(shell) == "sh"
  end

  test "review canary fails closed when the nested session does not initialize" do
    harness = capability_harness()
    System.put_env("SYMPHONY_REAL_CODEX_BIN", harness.real_codex)
    {:ok, context} = RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)

    assert {:error, {:reviewer_canary_not_initialized, 1}} =
             RunnerCapabilities.probe_review(context,
               command_runner: fn _executable, _args, _opts ->
                 {:ok, %{status: 1, output: "unable to start\n"}}
               end
             )
  end

  test "Camofox browser contract creates, captures, and cleans up a real tab shape" do
    harness = capability_harness()
    System.put_env("SYMPHONY_REAL_CODEX_BIN", harness.real_codex)
    {:ok, context} = RunnerCapabilities.prepare_context(harness.settings, wrapper_path: harness.wrapper)
    test_pid = self()

    request = fn options ->
      send(test_pid, {:browser_request, options})

      case {options[:method], URI.parse(options[:url]).path} do
        {:get, "/health"} -> {:ok, %{status: 200, body: %{ok: true}}}
        {:post, "/tabs"} -> {:ok, %{status: 201, body: %{"tabId" => "tab-123"}}}
        {:get, "/tabs/tab-123/screenshot"} -> {:ok, %{status: 200, body: png_fixture()}}
        {:delete, _path} -> {:ok, %{status: 204, body: ""}}
      end
    end

    assert :ok = RunnerCapabilities.probe_browser(context, request: request)

    assert_receive {:browser_request, health}
    assert health[:method] == :get
    assert health[:url] == "http://127.0.0.1:9377/health"
    assert_receive {:browser_request, create_tab}
    assert create_tab[:method] == :post
    assert create_tab[:json].url == "https://example.com"
    assert create_tab[:json].userId == create_tab[:json].sessionKey
    assert_receive {:browser_request, screenshot}
    assert screenshot[:url] =~ "/tabs/tab-123/screenshot?userId="
    assert_receive {:browser_request, delete_tab}
    assert delete_tab[:method] == :delete
    assert delete_tab[:url] =~ "/tabs/tab-123?userId="
    assert_receive {:browser_request, delete_session}
    assert delete_session[:method] == :delete
    assert delete_session[:url] =~ "/sessions/symphony-capability-canary-"
  end

  test "isolated wrapper preserves primary state for app-server and reviewer state for nested commands" do
    harness = capability_harness(create_reviewer_home: true)
    write_private_file(Path.join(harness.reviewer_home, "auth.json"), "test-auth\n")
    File.mkdir_p!(Path.join(harness.reviewer_home, "tmp"))
    File.chmod!(Path.join(harness.reviewer_home, "tmp"), 0o700)
    log_path = Path.join(harness.root, "codex.log")

    write_executable(
      harness.real_codex,
      "#!/usr/bin/env bash\nprintf '%s|%s|%s|%s\\n' \"${CODEX_HOME:-}\" \"${HOME:-}\" \"${TMPDIR:-}\" \"$*\" >> \"$TEST_CODEX_LOG\"\n"
    )

    env = [
      {"CODEX_HOME", harness.primary_home},
      {"HOME", harness.root},
      {"SYMPHONY_REAL_CODEX_BIN", harness.real_codex},
      {"SYMPHONY_REVIEW_CODEX_HOME", harness.reviewer_home},
      {"TEST_CODEX_LOG", log_path}
    ]

    assert {_output, 0} =
             System.cmd(
               harness.wrapper,
               ["--config", "foo=bar", "--model", "test-model", "app-server"],
               env: env
             )

    assert {_output, 0} = System.cmd(harness.wrapper, ["review", "--commit", "HEAD"], env: env)
    assert {_output, 0} = System.cmd(harness.wrapper, ["exec", "app-server"], env: env)

    [app_server, reviewer, nested_exec] = log_path |> File.read!() |> String.split("\n", trim: true)
    assert app_server =~ "#{harness.primary_home}|#{harness.root}|"
    assert app_server =~ "|--config foo=bar --model test-model app-server"
    assert reviewer =~ "#{harness.reviewer_home}|#{harness.reviewer_home}|#{harness.reviewer_home}/tmp|"
    assert reviewer =~ "review --commit HEAD"
    assert nested_exec =~ "#{harness.reviewer_home}|#{harness.reviewer_home}|#{harness.reviewer_home}/tmp|"
    assert nested_exec =~ "exec app-server"
  end

  defp capability_harness(opts \\ []) do
    temporary_root = Path.join(System.tmp_dir!(), "runner-capabilities-#{System.unique_integer([:positive])}")
    File.mkdir_p!(temporary_root)
    {:ok, root} = SymphonyElixir.PathSafety.canonicalize(temporary_root)
    source_repo = Path.join(root, "source")
    primary_home = Path.join(root, "primary-codex")
    reviewer_home = Path.join(root, "reviewer-codex")
    bin_dir = Path.join(root, "bin")
    wrapper = Application.app_dir(:symphony_elixir, "priv/codex-runner-wrapper.sh")
    real_codex = Path.join(bin_dir, "codex")
    sandbox_codex = Path.join(bin_dir, "sandbox-codex")

    File.mkdir_p!(Path.join(source_repo, ".git"))
    File.mkdir_p!(Path.join(primary_home, "skills/security-best-practices"))
    File.mkdir_p!(bin_dir)
    File.chmod!(root, 0o700)
    File.chmod!(primary_home, 0o700)
    write_private_file(Path.join(primary_home, "auth.json"), "test-auth\n")

    File.write!(
      Path.join(primary_home, "skills/security-best-practices/SKILL.md"),
      "---\nname: security-best-practices\n---\n"
    )

    write_executable(real_codex, "#!/usr/bin/env bash\nexit 0\n")
    write_executable(sandbox_codex, "#!/usr/bin/env bash\nexit 0\n")

    if Keyword.get(opts, :create_reviewer_home, false) do
      File.mkdir_p!(reviewer_home)
      File.chmod!(reviewer_home, 0o700)
    end

    {:ok, settings} =
      Schema.parse(%{
        tracker: %{kind: "memory"},
        workspace: %{root: Path.join(root, "workspaces")},
        codex: %{command: hardened_codex_command()},
        runner: %{
          capability_preflight: true,
          source_repo: source_repo,
          reviewer_codex_home: reviewer_home,
          primary_codex_home: primary_home,
          required_skills: ["security-best-practices"],
          sandbox_codex_bin: sandbox_codex,
          browser_backend: "camofox",
          browser_backend_url: "http://127.0.0.1:9377",
          review_timeout_ms: 20_000,
          browser_timeout_ms: 15_000,
          process_cleanup_timeout_ms: 500
        }
      })

    on_exit(fn -> File.rm_rf(root) end)

    %{
      root: root,
      source_repo: source_repo,
      primary_home: primary_home,
      reviewer_home: reviewer_home,
      wrapper: wrapper,
      real_codex: real_codex,
      sandbox_codex: sandbox_codex,
      settings: settings
    }
  end

  defp write_executable(path, content) do
    File.write!(path, content)
    File.chmod!(path, 0o755)
  end

  defp write_private_file(path, content) do
    File.write!(path, content)
    File.chmod!(path, 0o600)
  end

  defp file_mode(path), do: File.stat!(path).mode &&& 0o777
  defp png_fixture, do: <<0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, "test">>

  defp hardened_codex_command do
    [
      ~S|"${SYMPHONY_CODEX_BIN:-codex}"|,
      "--config shell_environment_policy.inherit=none",
      ~S|--config "shell_environment_policy.set.SYMPHONY_CODEX_BIN=\"${SYMPHONY_CODEX_BIN:-codex}\""|,
      ~S|--config "shell_environment_policy.set.SYMPHONY_REAL_CODEX_BIN=\"${SYMPHONY_REAL_CODEX_BIN}\""|,
      ~S|--config "shell_environment_policy.set.SYMPHONY_REVIEW_CODEX_HOME=\"${SYMPHONY_REVIEW_CODEX_HOME}\""|,
      ~S|--config "shell_environment_policy.set.SYMPHONY_BROWSER_BACKEND_URL=\"${SYMPHONY_BROWSER_BACKEND_URL}\""|,
      "app-server"
    ]
    |> Enum.join(" ")
  end
end
