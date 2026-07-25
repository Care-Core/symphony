defmodule SymphonyElixir.HoldStore do
  @moduledoc false

  import Bitwise, only: [band: 2]
  require Logger

  @filename ".symphony-holds.json"
  @version 1
  @private_file_mode 0o600

  @type hold :: %{
          required(:issue_id) => String.t(),
          required(:identifier) => String.t(),
          required(:reason) => String.t(),
          required(:limit) => pos_integer() | nil,
          required(:observed_tokens) => non_neg_integer(),
          required(:issue_state) => String.t() | nil,
          required(:held_at) => DateTime.t(),
          required(:cleanup_pending) => boolean(),
          optional(:warning_threshold) => pos_integer() | nil,
          optional(:warning_observed_at) => non_neg_integer() | nil,
          optional(:checkpoint_grace) => pos_integer() | nil,
          optional(:resume_phase) => String.t() | nil,
          optional(:requested_additional_input_tokens) => pos_integer() | nil,
          optional(:effective_additional_input_tokens) => pos_integer() | nil,
          optional(:attempt_input_token_baseline) => non_neg_integer(),
          optional(:input_token_tier_limit) => pos_integer() | nil,
          optional(:worker_host) => String.t() | nil,
          optional(:workspace_path) => Path.t() | nil,
          optional(:codex_app_server_pid) => pos_integer() | nil
        }

  @spec load(Path.t()) :: {:ok, %{optional(String.t()) => hold()}} | {:error, term()}
  def load(workspace_root) when is_binary(workspace_root) do
    case state_path(workspace_root) do
      {:ok, state_path} -> load_state_file(state_path)
      {:error, reason} -> {:error, reason}
    end
  end

  @spec persist(Path.t(), %{optional(String.t()) => hold()}) :: :ok | {:error, term()}
  def persist(workspace_root, holds) when is_binary(workspace_root) and is_map(holds) do
    with {:ok, state_path} <- state_path(workspace_root),
         {:ok, encoded} <- encode_state(holds) do
      atomic_private_write(state_path, encoded)
    end
  end

  defp load_state_file(state_path) do
    case File.lstat(state_path) do
      {:error, :enoent} ->
        {:ok, %{}}

      {:ok, %File.Stat{type: :regular} = initial_stat} ->
        load_regular_state_file(state_path, initial_stat)

      {:ok, %File.Stat{type: type}} ->
        {:error, {:hold_state_invalid_file, state_path, type}}

      {:error, reason} ->
        {:error, {:hold_state_unreadable, state_path, reason}}
    end
  end

  defp load_regular_state_file(state_path, initial_stat) do
    with :ok <- require_private_mode(state_path, initial_stat.mode),
         {:ok, encoded} <- read_open_state_file(state_path, initial_stat) do
      decode_state(encoded, state_path)
    end
  end

  defp read_open_state_file(state_path, initial_stat) do
    read_open_state_file(
      state_path,
      initial_stat,
      &read_validated_descriptor/3,
      &File.close/1
    )
  end

  @doc false
  @spec read_open_state_file_for_test(
          Path.t(),
          File.Stat.t(),
          (IO.device(), Path.t(), File.Stat.t() -> {:ok, binary()} | {:error, term()}),
          (IO.device() -> :ok | {:error, term()})
        ) :: {:ok, binary()} | {:error, term()}
  def read_open_state_file_for_test(state_path, initial_stat, descriptor_reader, descriptor_closer) do
    read_open_state_file(state_path, initial_stat, descriptor_reader, descriptor_closer)
  end

  defp read_open_state_file(state_path, initial_stat, descriptor_reader, descriptor_closer) do
    case File.open(state_path, [:read, :binary]) do
      {:ok, io_device} ->
        read_outcome =
          try do
            {:returned, descriptor_reader.(io_device, state_path, initial_stat)}
          catch
            kind, reason -> {:raised, kind, reason, __STACKTRACE__}
          end

        close_outcome =
          try do
            {:returned, descriptor_closer.(io_device)}
          catch
            kind, reason -> {:raised, kind, reason}
          end

        resolve_descriptor_outcomes(read_outcome, close_outcome, state_path)

      {:error, reason} ->
        {:error, {:hold_state_unreadable, state_path, reason}}
    end
  end

  defp resolve_descriptor_outcomes(
         {:raised, kind, reason, stacktrace},
         close_outcome,
         state_path
       ) do
    log_close_failure(close_outcome, state_path, "read raised")
    :erlang.raise(kind, reason, stacktrace)
  end

  defp resolve_descriptor_outcomes({:returned, read_result}, {:returned, :ok}, _state_path),
    do: read_result

  defp resolve_descriptor_outcomes(
         {:returned, {:error, read_reason}},
         close_outcome,
         state_path
       ) do
    {:error, close_reason} = close_failure(close_outcome)

    {:error, {:hold_state_descriptor_read_and_close_failed, state_path, read_reason, close_reason}}
  end

  defp resolve_descriptor_outcomes(
         {:returned, _read_result},
         close_outcome,
         state_path
       ) do
    {:error, reason} = close_failure(close_outcome)
    {:error, {:hold_state_descriptor_close_failed, state_path, reason}}
  end

  defp close_failure({:returned, {:error, reason}}), do: {:error, reason}
  defp close_failure({:returned, other}), do: {:error, {:unexpected_close_result, other}}
  defp close_failure({:raised, kind, reason}), do: {:error, {kind, reason}}

  defp log_close_failure(close_outcome, state_path, operation) do
    case close_outcome do
      {:returned, :ok} ->
        :ok

      _ ->
        {:error, reason} = close_failure(close_outcome)
        Logger.error("Hold state descriptor close failed after #{operation} state_path=#{state_path} reason=#{inspect(reason)}")
    end
  end

  @doc false
  @spec read_validated_descriptor(IO.device(), Path.t(), File.Stat.t()) ::
          {:ok, binary()} | {:error, term()}
  def read_validated_descriptor(io_device, state_path, initial_stat) do
    with {:ok, descriptor_stat} <- descriptor_stat(io_device, state_path),
         :ok <- validate_descriptor_stat(state_path, initial_stat, descriptor_stat) do
      read_descriptor(io_device, state_path)
    end
  end

  defp state_path(workspace_root) do
    expanded_root = Path.expand(workspace_root)

    case File.mkdir_p(expanded_root) do
      :ok -> {:ok, Path.join(expanded_root, @filename)}
      {:error, reason} -> {:error, {:hold_state_root_unavailable, expanded_root, reason}}
    end
  end

  defp descriptor_stat(io_device, state_path) do
    case :file.read_file_info(io_device) do
      {:ok, file_info} ->
        {:ok, File.Stat.from_record(file_info)}

      {:error, reason} ->
        {:error, {:hold_state_descriptor_stat_failed, state_path, reason}}
    end
  end

  defp validate_descriptor_stat(
         state_path,
         %File.Stat{major_device: initial_device, inode: initial_inode},
         %File.Stat{type: :regular, mode: mode, major_device: device, inode: inode}
       ) do
    with :ok <- require_private_descriptor_mode(state_path, mode),
         true <- device == initial_device and inode == initial_inode do
      :ok
    else
      false -> {:error, {:hold_state_identity_mismatch, state_path}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_descriptor_stat(state_path, _initial_stat, %File.Stat{type: type}) do
    {:error, {:hold_state_insecure_descriptor, state_path, {:invalid_type, type}}}
  end

  defp read_descriptor(io_device, state_path) do
    case IO.binread(io_device, :eof) do
      encoded when is_binary(encoded) -> {:ok, encoded}
      :eof -> {:ok, ""}
      {:error, reason} -> {:error, {:hold_state_descriptor_read_failed, state_path, reason}}
    end
  end

  defp decode_state(encoded, state_path) do
    case Jason.decode(encoded) do
      {:ok, %{"version" => @version, "holds" => holds}} when is_list(holds) ->
        decode_holds(holds, state_path)

      {:ok, _other} ->
        {:error, {:hold_state_invalid, state_path, :invalid_schema}}

      {:error, reason} ->
        {:error, {:hold_state_invalid, state_path, reason}}
    end
  end

  defp decode_holds(holds, state_path) do
    Enum.reduce_while(holds, {:ok, %{}}, fn encoded_hold, {:ok, decoded_holds} ->
      with {:ok, hold} <- decode_hold(encoded_hold),
           false <- Map.has_key?(decoded_holds, hold.issue_id) do
        {:cont, {:ok, Map.put(decoded_holds, hold.issue_id, hold)}}
      else
        true -> {:halt, {:error, {:hold_state_invalid, state_path, :duplicate_issue_id}}}
        {:error, reason} -> {:halt, {:error, {:hold_state_invalid, state_path, reason}}}
      end
    end)
  end

  defp decode_hold(
         %{
           "issue_id" => issue_id,
           "identifier" => identifier,
           "reason" => reason,
           "limit" => limit,
           "observed_tokens" => observed_tokens,
           "issue_state" => issue_state,
           "worker_host" => worker_host,
           "workspace_path" => workspace_path,
           "held_at" => held_at
         } = encoded_hold
       ) do
    cleanup_pending = Map.get(encoded_hold, "cleanup_pending", false)
    codex_app_server_pid = Map.get(encoded_hold, "codex_app_server_pid")
    warning_threshold = Map.get(encoded_hold, "warning_threshold")
    warning_observed_at = Map.get(encoded_hold, "warning_observed_at")
    checkpoint_grace = Map.get(encoded_hold, "checkpoint_grace")
    resume_phase = Map.get(encoded_hold, "resume_phase")

    requested_additional_input_tokens =
      Map.get(encoded_hold, "requested_additional_input_tokens")

    effective_additional_input_tokens =
      Map.get(encoded_hold, "effective_additional_input_tokens")

    attempt_input_token_baseline =
      Map.get(encoded_hold, "attempt_input_token_baseline", 0)

    input_token_tier_limit = Map.get(encoded_hold, "input_token_tier_limit")

    with :ok <- require_non_empty_string(issue_id, :issue_id),
         :ok <- require_non_empty_string(identifier, :identifier),
         :ok <- require_non_empty_string(reason, :reason),
         :ok <- require_optional_positive_integer(limit, :limit),
         :ok <- require_non_negative_integer(observed_tokens, :observed_tokens),
         :ok <- require_optional_string(issue_state, :issue_state),
         :ok <- require_optional_string(worker_host, :worker_host),
         :ok <- require_optional_string(workspace_path, :workspace_path),
         :ok <- require_boolean(cleanup_pending, :cleanup_pending),
         :ok <- require_optional_positive_integer(codex_app_server_pid, :codex_app_server_pid),
         :ok <- require_optional_positive_integer(warning_threshold, :warning_threshold),
         :ok <- require_optional_non_negative_integer(warning_observed_at, :warning_observed_at),
         :ok <- require_optional_positive_integer(checkpoint_grace, :checkpoint_grace),
         :ok <- require_optional_string(resume_phase, :resume_phase),
         :ok <-
           require_optional_positive_integer(
             requested_additional_input_tokens,
             :requested_additional_input_tokens
           ),
         :ok <-
           require_optional_positive_integer(
             effective_additional_input_tokens,
             :effective_additional_input_tokens
           ),
         :ok <-
           require_non_negative_integer(
             attempt_input_token_baseline,
             :attempt_input_token_baseline
           ),
         :ok <-
           require_optional_positive_integer(
             input_token_tier_limit,
             :input_token_tier_limit
           ),
         :ok <- require_non_empty_string(held_at, :held_at),
         {:ok, held_at, 0} <- DateTime.from_iso8601(held_at) do
      {:ok,
       %{
         issue_id: issue_id,
         identifier: identifier,
         reason: reason,
         limit: limit,
         observed_tokens: observed_tokens,
         issue_state: issue_state,
         worker_host: worker_host,
         workspace_path: workspace_path,
         codex_app_server_pid: codex_app_server_pid,
         warning_threshold: warning_threshold,
         warning_observed_at: warning_observed_at,
         checkpoint_grace: checkpoint_grace,
         resume_phase: resume_phase,
         requested_additional_input_tokens: requested_additional_input_tokens,
         effective_additional_input_tokens: effective_additional_input_tokens,
         attempt_input_token_baseline: attempt_input_token_baseline,
         input_token_tier_limit: input_token_tier_limit,
         cleanup_pending: cleanup_pending,
         held_at: held_at
       }}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_held_at}
    end
  end

  defp decode_hold(_hold), do: {:error, :invalid_hold}

  defp encode_state(holds) do
    payload = %{
      "version" => @version,
      "holds" => holds |> Map.values() |> Enum.sort_by(& &1.issue_id) |> Enum.map(&encode_hold/1)
    }

    case Jason.encode(payload) do
      {:ok, encoded} -> {:ok, encoded <> "\n"}
      {:error, reason} -> {:error, {:hold_state_encode_failed, reason}}
    end
  end

  defp encode_hold(hold) do
    %{
      "issue_id" => hold.issue_id,
      "identifier" => hold.identifier,
      "reason" => hold.reason,
      "limit" => hold.limit,
      "observed_tokens" => hold.observed_tokens,
      "warning_threshold" => Map.get(hold, :warning_threshold),
      "warning_observed_at" => Map.get(hold, :warning_observed_at),
      "checkpoint_grace" => Map.get(hold, :checkpoint_grace),
      "resume_phase" => Map.get(hold, :resume_phase),
      "requested_additional_input_tokens" => Map.get(hold, :requested_additional_input_tokens),
      "effective_additional_input_tokens" => Map.get(hold, :effective_additional_input_tokens),
      "attempt_input_token_baseline" => Map.get(hold, :attempt_input_token_baseline, 0),
      "input_token_tier_limit" => Map.get(hold, :input_token_tier_limit),
      "issue_state" => hold.issue_state,
      "worker_host" => Map.get(hold, :worker_host),
      "workspace_path" => Map.get(hold, :workspace_path),
      "codex_app_server_pid" => Map.get(hold, :codex_app_server_pid),
      "cleanup_pending" => Map.get(hold, :cleanup_pending, false),
      "held_at" => DateTime.to_iso8601(hold.held_at)
    }
  end

  @doc false
  @spec atomic_private_write_for_test(Path.t(), binary(), keyword()) :: :ok | {:error, term()}
  def atomic_private_write_for_test(state_path, encoded, opts)
      when is_binary(state_path) and is_binary(encoded) and is_list(opts) do
    atomic_private_write(state_path, encoded, opts)
  end

  defp atomic_private_write(state_path, encoded, opts \\ []) do
    temp_path = state_path <> ".tmp.#{System.unique_integer([:positive, :monotonic])}"
    descriptor_writer = Keyword.get(opts, :descriptor_writer, &write_descriptor/3)
    descriptor_closer = Keyword.get(opts, :descriptor_closer, &File.close/1)
    renamer = Keyword.get(opts, :renamer, &File.rename/2)

    try do
      result =
        case File.open(temp_path, [:write, :binary, :exclusive]) do
          {:ok, io_device} ->
            write_outcome = capture_outcome(fn -> descriptor_writer.(io_device, temp_path, encoded) end)
            close_outcome = capture_outcome(fn -> descriptor_closer.(io_device) end)
            resolve_write_outcomes(write_outcome, close_outcome, state_path)

          {:error, reason} ->
            {:error, reason}
        end

      case result do
        :ok ->
          case renamer.(temp_path, state_path) do
            :ok -> :ok
            {:error, reason} -> {:error, {:hold_state_write_failed, state_path, reason}}
          end

        {:error, reason} ->
          {:error, {:hold_state_write_failed, state_path, reason}}
      end
    after
      File.rm(temp_path)
    end
  end

  defp write_descriptor(io_device, temp_path, encoded) do
    with :ok <- File.chmod(temp_path, @private_file_mode),
         :ok <- IO.binwrite(io_device, encoded) do
      :file.sync(io_device)
    end
  end

  defp capture_outcome(operation) do
    {:returned, operation.()}
  catch
    kind, reason -> {:raised, kind, reason, __STACKTRACE__}
  end

  defp resolve_write_outcomes({:returned, write_result}, {:returned, :ok}, _state_path),
    do: write_result

  defp resolve_write_outcomes(
         {:returned, {:error, write_reason}},
         close_outcome,
         _state_path
       ) do
    {:error, close_reason} = close_failure_with_stack(close_outcome)

    {:error, {:temporary_descriptor_write_and_close_failed, write_reason, close_reason}}
  end

  defp resolve_write_outcomes({:returned, _write_result}, close_outcome, _state_path) do
    {:error, close_reason} = close_failure_with_stack(close_outcome)
    {:error, {:temporary_descriptor_close_failed, close_reason}}
  end

  defp resolve_write_outcomes(
         {:raised, kind, reason, stacktrace},
         close_outcome,
         state_path
       ) do
    log_write_close_failure(close_outcome, state_path)
    :erlang.raise(kind, reason, stacktrace)
  end

  defp close_failure_with_stack({:returned, {:error, reason}}), do: {:error, reason}
  defp close_failure_with_stack({:returned, other}), do: {:error, {:unexpected_close_result, other}}
  defp close_failure_with_stack({:raised, kind, reason, _stacktrace}), do: {:error, {kind, reason}}

  defp log_write_close_failure({:returned, :ok}, _state_path), do: :ok

  defp log_write_close_failure(close_outcome, state_path) do
    {:error, reason} = close_failure_with_stack(close_outcome)
    Logger.error("Hold state temporary descriptor close failed after write raised state_path=#{state_path} reason=#{inspect(reason)}")
  end

  defp require_private_mode(path, mode) do
    case band(mode, 0o777) do
      @private_file_mode -> :ok
      actual_mode -> {:error, {:hold_state_insecure_permissions, path, actual_mode}}
    end
  end

  defp require_private_descriptor_mode(path, mode) do
    case band(mode, 0o777) do
      @private_file_mode ->
        :ok

      actual_mode ->
        {:error, {:hold_state_insecure_descriptor, path, {:insecure_permissions, actual_mode}}}
    end
  end

  defp require_non_empty_string(value, _field) when is_binary(value) and byte_size(value) > 0, do: :ok
  defp require_non_empty_string(_value, field), do: {:error, {:invalid_field, field}}

  defp require_optional_positive_integer(nil, _field), do: :ok
  defp require_optional_positive_integer(value, _field) when is_integer(value) and value > 0, do: :ok
  defp require_optional_positive_integer(_value, field), do: {:error, {:invalid_field, field}}

  defp require_non_negative_integer(value, _field) when is_integer(value) and value >= 0, do: :ok
  defp require_non_negative_integer(_value, field), do: {:error, {:invalid_field, field}}

  defp require_optional_non_negative_integer(nil, _field), do: :ok

  defp require_optional_non_negative_integer(value, _field)
       when is_integer(value) and value >= 0,
       do: :ok

  defp require_optional_non_negative_integer(_value, field),
    do: {:error, {:invalid_field, field}}

  defp require_optional_string(nil, _field), do: :ok
  defp require_optional_string(value, _field) when is_binary(value), do: :ok
  defp require_optional_string(_value, field), do: {:error, {:invalid_field, field}}

  defp require_boolean(value, _field) when is_boolean(value), do: :ok
  defp require_boolean(_value, field), do: {:error, {:invalid_field, field}}
end
