defmodule SymphonyElixir.RunnerCapabilities do
  @moduledoc false

  import Bitwise

  alias SymphonyElixir.{Config, PathSafety, ProcessTree}

  @png_signature <<0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A>>
  @review_initialization_marker "OpenAI Codex v"
  @review_failure_markers [
    "failed to initialize in-process app-server client",
    "Operation not permitted"
  ]
  @required_shell_environment_variables [
    "SYMPHONY_CODEX_BIN",
    "SYMPHONY_REAL_CODEX_BIN",
    "SYMPHONY_REVIEW_CODEX_HOME",
    "SYMPHONY_BROWSER_BACKEND_URL"
  ]
  @codex_flags_with_value [
    "--enable",
    "--disable",
    "--model",
    "-m",
    "--local-provider",
    "--profile",
    "-p",
    "--sandbox",
    "-s",
    "--cd",
    "-C",
    "--add-dir",
    "--ask-for-approval",
    "-a"
  ]
  @codex_boolean_flags [
    "--strict-config",
    "--oss",
    "--dangerously-bypass-approvals-and-sandbox",
    "--dangerously-bypass-hook-trust",
    "--search",
    "--no-alt-screen"
  ]
  @unsafe_shell_syntax ~r/(?:^|\s)#|[\r\n;&|<>`]|\$\(/

  @type context :: %{
          browser_backend_url: String.t(),
          primary_codex_home: Path.t(),
          real_codex_bin: Path.t(),
          reviewer_codex_home: Path.t(),
          runner: struct(),
          source_repo: Path.t(),
          wrapper_codex_bin: Path.t()
        }

  @spec preflight(keyword()) :: :ok | {:error, term()}
  def preflight(opts \\ []) do
    settings = Keyword.get_lazy(opts, :settings, &Config.settings!/0)

    if settings.runner.capability_preflight do
      review_probe = Keyword.get(opts, :review_probe, &probe_review/1)
      browser_probe = Keyword.get(opts, :browser_probe, &probe_browser/1)

      with {:ok, context} <- prepare_context(settings, opts),
           :ok <- review_probe.(context) do
        browser_probe.(context)
      end
    else
      :ok
    end
  end

  @doc false
  @spec prepare_context(struct(), keyword()) :: {:ok, context()} | {:error, term()}
  def prepare_context(settings, opts \\ []) do
    wrapper_path = Keyword.get_lazy(opts, :wrapper_path, &default_wrapper_path/0)

    with :ok <- validate_local_runner(settings),
         :ok <- validate_codex_shell_environment(settings.codex.command),
         :ok <- ProcessTree.validate_launcher(),
         {:ok, source_repo} <- canonical_private_source_repo(settings.runner.source_repo),
         {:ok, wrapper_codex_bin} <- require_executable(wrapper_path, :runner_wrapper),
         {:ok, real_codex_bin} <- resolve_real_codex(wrapper_codex_bin),
         {:ok, primary_codex_home} <- canonical_directory(settings.runner.primary_codex_home, :primary_codex_home),
         {:ok, reviewer_codex_home} <-
           prepare_reviewer_home(primary_codex_home, settings.runner.reviewer_codex_home),
         :ok <- copy_required_skills(primary_codex_home, reviewer_codex_home, settings.runner.required_skills),
         {:ok, sandbox_codex_bin} <- require_executable(settings.runner.sandbox_codex_bin, :sandbox_codex_bin),
         {:ok, browser_backend_url} <- validate_browser_backend(settings.runner) do
      runner = %{settings.runner | sandbox_codex_bin: sandbox_codex_bin}

      context = %{
        browser_backend_url: browser_backend_url,
        primary_codex_home: primary_codex_home,
        real_codex_bin: real_codex_bin,
        reviewer_codex_home: reviewer_codex_home,
        runner: runner,
        source_repo: source_repo,
        wrapper_codex_bin: wrapper_codex_bin
      }

      install_runtime_environment(context)
      {:ok, context}
    end
  end

  @doc false
  @spec probe_review(context(), keyword()) :: :ok | {:error, term()}
  def probe_review(context, opts \\ []) do
    command_runner = Keyword.get(opts, :command_runner, &ProcessTree.run/3)
    runner = context.runner
    permissions_profile = sandbox_permissions_profile(context)

    args =
      [
        "sandbox",
        "--log-denials",
        "-c",
        "default_permissions=\"symphony_canary\"",
        "-c",
        permissions_profile,
        "-P",
        "symphony_canary",
        "-C",
        context.source_repo,
        "--",
        env_executable(),
        "-i"
      ] ++
        review_environment(context) ++
        [shell_executable(), "-c", ~S|exec "$SYMPHONY_CODEX_BIN" "$@"|, "symphony-canary", "review", "--commit", "HEAD"]

    case command_runner.(runner.sandbox_codex_bin, args,
           cd: context.source_repo,
           timeout_ms: runner.review_timeout_ms,
           max_output_bytes: 64_000
         ) do
      {:ok, %{status: status, output: output}} ->
        validate_review_canary_output(output, status)

      {:error, {:timeout, timeout_ms, output}} ->
        case validate_review_canary_output(output, :timeout) do
          :ok -> :ok
          {:error, _reason} -> {:error, {:reviewer_canary_timeout, timeout_ms}}
        end

      {:error, reason} ->
        {:error, {:reviewer_canary_start_failed, reason}}
    end
  end

  @doc false
  @spec probe_browser(context(), keyword()) :: :ok | {:error, term()}
  def probe_browser(context, opts \\ []) do
    request = Keyword.get(opts, :request, &Req.request/1)
    user_id = "symphony-capability-canary-#{System.unique_integer([:positive])}"

    with :ok <- browser_health_check(context, request),
         {:ok, tab_id} <- create_browser_tab(context, request, user_id) do
      try do
        capture_browser_screenshot(context, request, user_id, tab_id)
      after
        cleanup_browser_session(context, request, user_id, tab_id)
      end
    else
      {:error, reason} ->
        cleanup_browser_session(context, request, user_id, nil)
        {:error, reason}
    end
  end

  defp validate_local_runner(settings) do
    cond do
      settings.worker.ssh_hosts != [] ->
        {:error, :runner_capability_preflight_requires_local_worker}

      Path.type(settings.runner.source_repo || "") != :absolute ->
        {:error, :runner_source_repo_must_be_absolute}

      Path.type(settings.runner.reviewer_codex_home || "") != :absolute ->
        {:error, :reviewer_codex_home_must_be_absolute}

      Path.type(settings.runner.primary_codex_home || "") != :absolute ->
        {:error, :primary_codex_home_must_be_absolute}

      true ->
        :ok
    end
  end

  defp validate_codex_shell_environment(command) when is_binary(command) do
    with {:ok, wrapper, args, probe_values} <- expand_codex_command(command),
         {:ok, global_args} <- require_app_server_subcommand(args),
         {:ok, effective_config} <- effective_codex_config(global_args),
         :ok <- require_disabled_environment_inheritance(effective_config),
         :ok <- require_wrapper_command(wrapper, probe_values) do
      validate_required_shell_environment(effective_config, probe_values)
    end
  end

  defp validate_codex_shell_environment(_command), do: {:error, :codex_command_missing}

  defp expand_codex_command(command) do
    probe_values = shell_environment_probe_values()

    with :ok <- validate_shell_syntax(command),
         {output, 0} <-
           System.cmd(shell_executable(), ["-c", "set -- #{command}\nprintf '%s\\0' \"$@\""],
             env: Map.to_list(probe_values),
             stderr_to_stdout: true
           ) do
      case String.split(output, <<0>>, trim: true) do
        ["exec", wrapper | args] -> {:ok, wrapper, args, probe_values}
        [wrapper | args] -> {:ok, wrapper, args, probe_values}
        _ -> {:error, :codex_command_missing}
      end
    else
      {_output, status} when is_integer(status) -> {:error, :codex_command_invalid_shell_syntax}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :codex_command_invalid_shell_syntax}
  end

  defp validate_shell_syntax(command) do
    unsupported_expansion =
      @required_shell_environment_variables
      |> Enum.flat_map(fn variable -> ["${#{variable}:-codex}", "${#{variable}}", "$#{variable}"] end)
      |> Enum.reduce(command, &String.replace(&2, &1, ""))
      |> String.contains?("$")

    cond do
      Regex.match?(@unsafe_shell_syntax, command) -> {:error, :codex_command_contains_unsafe_shell_syntax}
      unsupported_expansion -> {:error, :codex_command_contains_unsupported_shell_expansion}
      true -> :ok
    end
  end

  defp shell_environment_probe_values do
    suffix = System.unique_integer([:positive])
    Map.new(@required_shell_environment_variables, &{&1, "__symphony_preflight_#{suffix}_#{&1}__"})
  end

  defp require_wrapper_command(wrapper, probe_values) do
    if wrapper == Map.fetch!(probe_values, "SYMPHONY_CODEX_BIN") do
      :ok
    else
      {:error, :codex_command_must_use_symphony_wrapper}
    end
  end

  defp require_app_server_subcommand(args) do
    if List.last(args) == "app-server" do
      {:ok, Enum.drop(args, -1)}
    else
      {:error, :codex_command_must_run_app_server}
    end
  end

  defp effective_codex_config(args), do: collect_codex_config(args, %{})

  defp collect_codex_config([], config), do: {:ok, config}

  defp collect_codex_config([flag, assignment | rest], config) when flag in ["--config", "-c"] do
    with {:ok, key, value} <- split_config_assignment(assignment) do
      collect_codex_config(rest, Map.put(config, key, value))
    end
  end

  defp collect_codex_config(["--config=" <> assignment | rest], config) do
    with {:ok, key, value} <- split_config_assignment(assignment) do
      collect_codex_config(rest, Map.put(config, key, value))
    end
  end

  defp collect_codex_config(["-c" <> assignment | rest], config) when assignment != "" do
    with {:ok, key, value} <- split_config_assignment(assignment) do
      collect_codex_config(rest, Map.put(config, key, value))
    end
  end

  defp collect_codex_config([flag], _config) when flag in ["--config", "-c"] do
    {:error, {:codex_command_config_value_missing, flag}}
  end

  defp collect_codex_config([flag, _value | rest], config) when flag in @codex_flags_with_value do
    collect_codex_config(rest, config)
  end

  defp collect_codex_config([flag | rest], config) when flag in @codex_boolean_flags do
    collect_codex_config(rest, config)
  end

  defp collect_codex_config([arg | rest], config) do
    if supported_codex_flag_with_inline_value?(arg) do
      collect_codex_config(rest, config)
    else
      {:error, {:codex_command_unsupported_argument, arg}}
    end
  end

  defp supported_codex_flag_with_inline_value?(arg) do
    Enum.any?(@codex_flags_with_value, fn
      "--" <> _rest = flag -> String.starts_with?(arg, flag <> "=")
      "-" <> _rest = flag -> String.starts_with?(arg, flag) and byte_size(arg) > byte_size(flag)
    end)
  end

  defp split_config_assignment(assignment) do
    case String.split(assignment, "=", parts: 2) do
      [key, value] when key != "" -> {:ok, key, value}
      _ -> {:error, {:codex_command_invalid_config_assignment, assignment}}
    end
  end

  defp require_disabled_environment_inheritance(config) do
    if Map.get(config, "shell_environment_policy.inherit") == "none" do
      :ok
    else
      {:error, :codex_command_must_disable_shell_environment_inheritance}
    end
  end

  defp validate_required_shell_environment(config, probe_values) do
    missing_variables =
      Enum.reject(@required_shell_environment_variables, fn variable ->
        Map.has_key?(config, "shell_environment_policy.set.#{variable}")
      end)

    case missing_variables do
      [] -> validate_shell_environment_values(config, probe_values)
      variables -> {:error, {:codex_command_missing_shell_environment, variables}}
    end
  end

  defp validate_shell_environment_values(config, probe_values) do
    invalid_variable =
      Enum.find(@required_shell_environment_variables, fn variable ->
        Map.fetch!(config, "shell_environment_policy.set.#{variable}") !=
          ~s|"#{Map.fetch!(probe_values, variable)}"|
      end)

    case invalid_variable do
      nil -> :ok
      variable -> {:error, {:codex_command_invalid_shell_environment, variable}}
    end
  end

  defp canonical_private_source_repo(path) do
    with {:ok, canonical_path} <- canonical_directory(path, :source_repo),
         true <- File.exists?(Path.join(canonical_path, ".git")) or {:error, {:source_repo_not_git, canonical_path}} do
      {:ok, canonical_path}
    end
  end

  defp canonical_directory(path, label) when is_binary(path) do
    with {:ok, canonical_path} <- PathSafety.canonicalize(path),
         {:ok, %File.Stat{type: :directory}} <- File.stat(canonical_path) do
      {:ok, canonical_path}
    else
      {:ok, %File.Stat{type: type}} -> {:error, {label, :not_directory, type}}
      {:error, reason} -> {:error, {label, reason}}
    end
  end

  defp canonical_directory(path, label), do: {:error, {label, :missing_path, path}}

  defp prepare_reviewer_home(primary_codex_home, reviewer_home) do
    expanded_reviewer_home = Path.expand(reviewer_home)
    parent = Path.dirname(expanded_reviewer_home)
    reviewer_home_name = Path.basename(expanded_reviewer_home)

    with {:ok, current_uid} <- current_uid(),
         {:ok, canonical_parent} <- PathSafety.canonicalize(parent),
         :ok <- require_private_directory(canonical_parent, current_uid),
         canonical_reviewer_home = Path.join(canonical_parent, reviewer_home_name),
         :ok <- require_isolated_reviewer_home(primary_codex_home, canonical_reviewer_home),
         :ok <- ensure_private_directory(canonical_reviewer_home, current_uid),
         :ok <- ensure_private_directory(Path.join(canonical_reviewer_home, "tmp"), current_uid),
         :ok <- reject_auth_symlink(Path.join(canonical_reviewer_home, "auth.json")),
         primary_auth = Path.join(primary_codex_home, "auth.json"),
         :ok <- require_private_file(primary_auth, current_uid),
         :ok <- atomic_copy_auth(primary_auth, canonical_reviewer_home, current_uid) do
      {:ok, canonical_reviewer_home}
    end
  end

  defp require_isolated_reviewer_home(primary_codex_home, reviewer_codex_home) do
    if overlapping_paths?(primary_codex_home, reviewer_codex_home) do
      {:error, {:reviewer_codex_home_not_isolated, reviewer_codex_home}}
    else
      :ok
    end
  end

  defp overlapping_paths?(left, right) do
    left_parts = normalized_path_parts(left)
    right_parts = normalized_path_parts(right)

    {shorter, longer} =
      if length(left_parts) <= length(right_parts), do: {left_parts, right_parts}, else: {right_parts, left_parts}

    Enum.take(longer, length(shorter)) == shorter
  end

  defp normalized_path_parts(path) do
    path
    |> Path.split()
    |> Enum.map(&normalize_path_component/1)
  end

  defp normalize_path_component(component) do
    case :os.type() do
      {:unix, :darwin} -> component |> String.normalize(:nfd) |> String.downcase()
      _ -> component
    end
  end

  defp require_private_directory(path, current_uid) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: ^current_uid, mode: mode}} ->
        require_mode(path, mode, 0o700)

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, {:private_directory_symlink, path}}

      {:ok, %File.Stat{type: type}} ->
        {:error, {:private_directory_invalid, path, type}}

      {:error, reason} ->
        {:error, {:private_directory_unreadable, path, reason}}
    end
  end

  defp ensure_private_directory(path, current_uid) do
    case File.lstat(path) do
      {:ok, _stat} ->
        require_private_directory(path, current_uid)

      {:error, :enoent} ->
        with {:ok, mkdir} <- find_executable("mkdir"),
             {_output, 0} <- System.cmd(mkdir, ["-m", "700", path], stderr_to_stdout: true) do
          require_private_directory(path, current_uid)
        else
          {:error, reason} -> {:error, reason}
          {_output, status} -> {:error, {:private_directory_create_failed, path, status}}
        end

      {:error, reason} ->
        {:error, {:private_directory_unreadable, path, reason}}
    end
  end

  defp require_private_file(path, current_uid) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular, uid: ^current_uid, mode: mode}} ->
        if permission_mode(mode) in [0o600, 0o400] do
          :ok
        else
          {:error, {:private_file_mode, path, permission_mode(mode)}}
        end

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, {:private_file_symlink, path}}

      {:ok, %File.Stat{type: type}} ->
        {:error, {:private_file_invalid, path, type}}

      {:error, reason} ->
        {:error, {:private_file_unreadable, path, reason}}
    end
  end

  defp reject_auth_symlink(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} -> {:error, {:reviewer_auth_symlink, path}}
      {:ok, %File.Stat{type: :regular}} -> :ok
      {:ok, %File.Stat{type: type}} -> {:error, {:reviewer_auth_invalid, path, type}}
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:reviewer_auth_unreadable, path, reason}}
    end
  end

  defp atomic_copy_auth(primary_auth, reviewer_home, current_uid) do
    auth_target = Path.join(reviewer_home, "auth.json")
    template = Path.join(reviewer_home, ".auth.json.XXXXXX")

    with {:ok, auth_temp} <- create_private_temp_file(template, current_uid) do
      try do
        with :ok <- File.cp(primary_auth, auth_temp),
             :ok <- File.chmod(auth_temp, 0o600),
             :ok <- require_private_file(auth_temp, current_uid),
             :ok <- File.rename(auth_temp, auth_target) do
          require_private_file(auth_target, current_uid)
        end
      after
        File.rm(auth_temp)
      end
    end
  end

  defp create_private_temp_file(template, current_uid) do
    with {:ok, mktemp} <- find_executable("mktemp"),
         {output, 0} <- System.cmd(mktemp, [template], stderr_to_stdout: true),
         temp_path = String.trim(output),
         true <- Path.dirname(temp_path) == Path.dirname(template) or {:error, {:unsafe_temp_path, temp_path}},
         :ok <- require_mode_and_owner(temp_path, current_uid, 0o600) do
      {:ok, temp_path}
    else
      {:error, reason} -> {:error, reason}
      {_output, status} -> {:error, {:mktemp_failed, status}}
    end
  end

  defp require_mode_and_owner(path, current_uid, expected_mode) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular, uid: ^current_uid, mode: mode}} ->
        require_mode(path, mode, expected_mode)

      {:ok, %File.Stat{type: type}} ->
        {:error, {:temporary_file_invalid, path, type}}

      {:error, reason} ->
        {:error, {:temporary_file_unreadable, path, reason}}
    end
  end

  defp require_mode(path, mode, expected_mode) do
    if permission_mode(mode) == expected_mode do
      :ok
    else
      {:error, {:unexpected_mode, path, permission_mode(mode), expected_mode}}
    end
  end

  defp permission_mode(mode), do: mode &&& 0o777

  defp current_uid do
    with {:ok, id} <- find_executable("id"),
         {output, 0} <- System.cmd(id, ["-u"], stderr_to_stdout: true),
         {uid, ""} <- output |> String.trim() |> Integer.parse() do
      {:ok, uid}
    else
      {:error, reason} -> {:error, reason}
      {_output, status} -> {:error, {:current_uid_failed, status}}
      :error -> {:error, :current_uid_invalid}
    end
  end

  defp copy_required_skills(_primary_home, _reviewer_home, []), do: :ok

  defp copy_required_skills(primary_home, reviewer_home, required_skills) do
    with {:ok, current_uid} <- current_uid(),
         skills_root = Path.join(reviewer_home, "skills"),
         :ok <- ensure_private_directory(skills_root, current_uid) do
      Enum.reduce_while(required_skills, :ok, fn skill_name, :ok ->
        copy_skill_until_error(primary_home, skills_root, skill_name, current_uid)
      end)
    end
  end

  defp copy_skill_until_error(primary_home, skills_root, skill_name, current_uid) do
    case copy_required_skill(primary_home, skills_root, skill_name, current_uid) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp copy_required_skill(primary_home, reviewer_skills_root, skill_name, current_uid) do
    with :ok <- validate_skill_name(skill_name),
         source = Path.join([primary_home, "skills", skill_name]),
         :ok <- require_skill_source(source),
         target = Path.join(reviewer_skills_root, skill_name),
         :ok <- reject_symlink_tree(target, allow_missing: true),
         temp = Path.join(reviewer_skills_root, ".#{skill_name}.#{System.unique_integer([:positive])}"),
         :ok <- File.mkdir(temp),
         :ok <- File.chmod(temp, 0o700) do
      try do
        with :ok <- copy_private_tree(source, temp),
             {:ok, _removed} <- File.rm_rf(target),
             :ok <- File.rename(temp, target),
             :ok <- require_private_directory(target, current_uid) do
          :ok
        else
          {:error, _file, _reason} = error -> {:error, {:skill_target_remove_failed, skill_name, error}}
          {:error, reason} -> {:error, reason}
        end
      after
        File.rm_rf(temp)
      end
    end
  end

  defp validate_skill_name(skill_name) when is_binary(skill_name) do
    if skill_name not in [".", ".."] and String.match?(skill_name, ~r/^[A-Za-z0-9._-]+$/) do
      :ok
    else
      {:error, {:invalid_required_skill, skill_name}}
    end
  end

  defp validate_skill_name(skill_name), do: {:error, {:invalid_required_skill, skill_name}}

  defp require_skill_source(source) do
    with {:ok, %File.Stat{type: :directory}} <- File.lstat(source),
         true <- File.regular?(Path.join(source, "SKILL.md")) or {:error, {:required_skill_missing_manifest, source}},
         :ok <- reject_symlink_tree(source) do
      :ok
    else
      {:ok, %File.Stat{type: type}} -> {:error, {:required_skill_invalid, source, type}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reject_symlink_tree(path, opts \\ []) do
    allow_missing = Keyword.get(opts, :allow_missing, false)

    case File.lstat(path) do
      {:error, :enoent} when allow_missing ->
        :ok

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, {:symlink_not_allowed, path}}

      {:ok, %File.Stat{type: :directory}} ->
        reduce_directory(path, fn entry_path -> reject_symlink_tree(entry_path, opts) end)

      {:ok, %File.Stat{type: :regular}} ->
        :ok

      {:ok, %File.Stat{type: type}} ->
        {:error, {:unsupported_file_type, path, type}}

      {:error, reason} ->
        {:error, {:path_unreadable, path, reason}}
    end
  end

  defp copy_private_tree(source, target) do
    reduce_directory(source, fn source_entry ->
      target_entry = Path.join(target, Path.basename(source_entry))
      copy_private_entry(source_entry, target_entry)
    end)
  end

  defp copy_private_entry(source, target) do
    case File.lstat(source) do
      {:ok, %File.Stat{type: :directory}} ->
        copy_private_directory(source, target)

      {:ok, %File.Stat{type: :regular, mode: source_mode}} ->
        target_mode = if (source_mode &&& 0o100) == 0o100, do: 0o700, else: 0o600

        with :ok <- File.cp(source, target),
             do: File.chmod(target, target_mode)

      {:ok, %File.Stat{type: type}} ->
        {:error, {:unsupported_file_type, source, type}}

      {:error, reason} ->
        {:error, {:path_unreadable, source, reason}}
    end
  end

  defp copy_private_directory(source, target) do
    with :ok <- File.mkdir(target),
         :ok <- File.chmod(target, 0o700),
         do: copy_private_tree(source, target)
  end

  defp reduce_directory(path, operation) do
    case File.ls(path) do
      {:ok, entries} -> reduce_entries(path, entries, operation)
      {:error, reason} -> {:error, {:path_unreadable, path, reason}}
    end
  end

  defp reduce_entries(path, entries, operation) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case operation.(Path.join(path, entry)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp resolve_real_codex(wrapper_path) do
    configured_real = normalize_env_value(System.get_env("SYMPHONY_REAL_CODEX_BIN"))
    configured_legacy = normalize_env_value(System.get_env("SYMPHONY_CODEX_BIN"))

    candidate =
      configured_real ||
        if(configured_legacy && not same_path?(configured_legacy, wrapper_path), do: configured_legacy) ||
        "codex"

    require_executable(candidate, :real_codex_bin)
  end

  defp require_executable(path_or_name, label) when is_binary(path_or_name) do
    resolved =
      case Path.type(path_or_name) do
        :absolute -> path_or_name
        _ -> System.find_executable(path_or_name)
      end

    case resolved do
      path when is_binary(path) ->
        case File.stat(path) do
          {:ok, %File.Stat{type: :regular, mode: mode}} when (mode &&& 0o111) != 0 ->
            {:ok, Path.expand(path)}

          {:ok, %File.Stat{type: type}} ->
            {:error, {label, :not_executable, path, type}}

          {:error, reason} ->
            {:error, {label, :unreadable, path, reason}}
        end

      _ ->
        {:error, {label, :not_found, path_or_name}}
    end
  end

  defp require_executable(path_or_name, label), do: {:error, {label, :missing, path_or_name}}

  defp same_path?(left, right) do
    Path.expand(left) == Path.expand(right)
  rescue
    _ -> false
  end

  defp normalize_env_value(nil), do: nil

  defp normalize_env_value(value) do
    case String.trim(value) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp validate_browser_backend(runner) do
    with "camofox" <- runner.browser_backend,
         browser_url when is_binary(browser_url) <- normalize_env_value(runner.browser_backend_url),
         %URI{scheme: scheme, host: host} <- URI.parse(browser_url),
         true <- scheme in ["http", "https"] and host in ["localhost", "127.0.0.1", "::1"] do
      {:ok, String.trim_trailing(browser_url, "/")}
    else
      _ -> {:error, :invalid_camofox_browser_backend}
    end
  end

  defp install_runtime_environment(context) do
    System.put_env("CODEX_HOME", context.primary_codex_home)
    System.put_env("SYMPHONY_REAL_CODEX_BIN", context.real_codex_bin)
    System.put_env("SYMPHONY_CODEX_BIN", context.wrapper_codex_bin)
    System.put_env("SYMPHONY_REVIEW_CODEX_HOME", context.reviewer_codex_home)
    System.put_env("SYMPHONY_BROWSER_BACKEND_URL", context.browser_backend_url)
    :ok
  end

  defp sandbox_permissions_profile(context) do
    home_root = Path.expand("~")

    filesystem =
      [
        {":root", "read"},
        {home_root, "deny"},
        {context.source_repo, "read"},
        {context.wrapper_codex_bin, "read"},
        {context.reviewer_codex_home, "write"},
        {":tmpdir", "write"},
        {":slash_tmp", "write"}
      ]
      |> Enum.uniq()
      |> Enum.map_join(", ", fn {path, access} ->
        "#{toml_string(path)} = #{toml_string(access)}"
      end)

    "permissions.symphony_canary={filesystem = {#{filesystem}}, network = {enabled = false}}"
  end

  defp toml_string(value) do
    escaped = value |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")
    "\"#{escaped}\""
  end

  defp review_environment(context) do
    path =
      [Path.dirname(context.real_codex_bin), "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"]
      |> Enum.uniq()
      |> Enum.join(":")

    [
      "PATH=#{path}",
      "HOME=#{context.reviewer_codex_home}",
      "TMPDIR=#{Path.join(context.reviewer_codex_home, "tmp")}",
      "LANG=#{System.get_env("LANG") || "en_US.UTF-8"}",
      "SYMPHONY_CODEX_BIN=#{context.wrapper_codex_bin}",
      "SYMPHONY_REAL_CODEX_BIN=#{context.real_codex_bin}",
      "SYMPHONY_REVIEW_CODEX_HOME=#{context.reviewer_codex_home}",
      "SYMPHONY_BROWSER_BACKEND_URL=#{context.browser_backend_url}"
    ]
  end

  defp validate_review_canary_output(output, status) do
    cond do
      Enum.any?(@review_failure_markers, &String.contains?(output, &1)) ->
        {:error, {:reviewer_canary_sandbox_denied, status}}

      not String.contains?(output, @review_initialization_marker) ->
        {:error, {:reviewer_canary_not_initialized, status}}

      true ->
        :ok
    end
  end

  defp browser_health_check(context, request) do
    case request.(browser_request(context, :get, "/health")) do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status}} -> {:error, {:browser_backend_unhealthy, status}}
      {:error, reason} -> {:error, {:browser_backend_unavailable, reason}}
    end
  end

  defp create_browser_tab(context, request, user_id) do
    options =
      browser_request(context, :post, "/tabs")
      |> Keyword.put(:json, %{userId: user_id, sessionKey: user_id, url: "https://example.com"})

    case request.(options) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        case body do
          %{"tabId" => tab_id} when is_binary(tab_id) and tab_id != "" -> {:ok, tab_id}
          %{tabId: tab_id} when is_binary(tab_id) and tab_id != "" -> {:ok, tab_id}
          _ -> {:error, :browser_backend_missing_tab_id}
        end

      {:ok, %{status: status}} ->
        {:error, {:browser_backend_tab_failed, status}}

      {:error, reason} ->
        {:error, {:browser_backend_tab_unavailable, reason}}
    end
  end

  defp capture_browser_screenshot(context, request, user_id, tab_id) do
    query = URI.encode_query(%{"userId" => user_id})
    path = "/tabs/#{encode_path_segment(tab_id)}/screenshot?#{query}"

    case request.(browser_request(context, :get, path)) do
      {:ok, %{status: status, body: <<@png_signature, _rest::binary>>}} when status in 200..299 ->
        :ok

      {:ok, %{status: status}} when status in 200..299 ->
        {:error, :browser_backend_invalid_screenshot}

      {:ok, %{status: status}} ->
        {:error, {:browser_backend_screenshot_failed, status}}

      {:error, reason} ->
        {:error, {:browser_backend_screenshot_unavailable, reason}}
    end
  end

  defp cleanup_browser_session(context, request, user_id, tab_id) do
    query = URI.encode_query(%{"userId" => user_id})

    if is_binary(tab_id) do
      safe_browser_request(request, browser_request(context, :delete, "/tabs/#{encode_path_segment(tab_id)}?#{query}"))
    end

    safe_browser_request(request, browser_request(context, :delete, "/sessions/#{URI.encode(user_id)}"))
  end

  defp safe_browser_request(request, options) do
    request.(options)
    :ok
  rescue
    _ -> :ok
  end

  defp browser_request(context, method, path) do
    [
      method: method,
      url: context.browser_backend_url <> path,
      connect_options: [timeout: context.runner.browser_timeout_ms],
      receive_timeout: context.runner.browser_timeout_ms,
      retry: false
    ]
  end

  defp find_executable(name) do
    case System.find_executable(name) do
      nil -> {:error, {:command_not_found, name}}
      path -> {:ok, path}
    end
  end

  defp env_executable do
    System.find_executable("env") || "/usr/bin/env"
  end

  defp shell_executable do
    System.find_executable("sh") || "/bin/sh"
  end

  defp encode_path_segment(value), do: URI.encode(value, &URI.char_unreserved?/1)

  defp default_wrapper_path do
    Application.app_dir(:symphony_elixir, "priv/codex-runner-wrapper.sh")
  end
end
