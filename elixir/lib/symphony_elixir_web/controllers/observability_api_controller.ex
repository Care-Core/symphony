defmodule SymphonyElixirWeb.ObservabilityApiController do
  @moduledoc """
  JSON API for Symphony observability data.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixir.{ControlToken, IssueCapability}
  alias SymphonyElixirWeb.{Endpoint, Presenter}

  @issue_signature_header "x-symphony-issue-signature"

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    with_control_access(conn, fn ->
      json(conn, Presenter.state_payload(orchestrator(), snapshot_timeout_ms()))
    end)
  end

  @spec issue(Conn.t(), map()) :: Conn.t()
  def issue(conn, %{"issue_identifier" => issue_identifier}) do
    with_control_access(conn, fn ->
      case Presenter.issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
        {:ok, payload} ->
          json(conn, payload)

        {:error, :issue_not_found} ->
          error_response(conn, 404, "issue_not_found", "Issue not found")
      end
    end)
  end

  @spec refresh(Conn.t(), map()) :: Conn.t()
  def refresh(conn, _params) do
    case Presenter.refresh_payload(orchestrator()) do
      {:ok, payload} ->
        conn
        |> put_status(202)
        |> json(payload)

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")
    end
  end

  @spec stop(Conn.t(), map()) :: Conn.t()
  def stop(conn, %{"issue_identifier" => issue_identifier}) do
    with :ok <- require_control_access(conn),
         {:ok, payload} <- Presenter.stop_payload(issue_identifier, orchestrator()) do
      json(conn, payload)
    else
      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")

      {:error, :cleanup_failed} ->
        error_response(conn, 503, "cleanup_failed", "The remote run could not be confirmed stopped")

      {:error, :hold_state_unavailable} ->
        error_response(conn, 503, "hold_state_unavailable", "Durable hold state is unavailable")

      {:error, :loopback_only} ->
        error_response(conn, 403, "loopback_only", "Control endpoints are available only on loopback")

      {:error, :control_token_not_configured} ->
        error_response(conn, 503, "control_token_not_configured", "Control token is not configured")

      {:error, :invalid_control_token} ->
        error_response(conn, 401, "invalid_control_token", "Invalid control token")
    end
  end

  @spec resume(Conn.t(), map()) :: Conn.t()
  def resume(conn, %{"issue_identifier" => issue_identifier} = params) do
    options = Map.take(params, ["phase", "max_additional_input_tokens"])

    with :ok <- require_control_access(conn),
         {:ok, payload} <- Presenter.resume_payload(issue_identifier, options, orchestrator()) do
      json(conn, payload)
    else
      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")

      {:error, :cleanup_failed} ->
        error_response(conn, 503, "cleanup_failed", "Cleanup could not be confirmed; the hold remains active")

      {:error, :hold_state_unavailable} ->
        error_response(conn, 503, "hold_state_unavailable", "Durable hold state is unavailable")

      {:error, :resume_phase_required} ->
        error_response(conn, 422, "resume_phase_required", "A named resume phase is required for this token-budget hold")

      {:error, :invalid_resume_phase} ->
        error_response(conn, 422, "invalid_resume_phase", "Resume phase is not supported")

      {:error, :max_additional_input_tokens_required} ->
        error_response(conn, 422, "max_additional_input_tokens_required", "A maximum additional input-token allowance is required")

      {:error, :invalid_max_additional_input_tokens} ->
        error_response(conn, 422, "invalid_max_additional_input_tokens", "Maximum additional input tokens must be a positive integer")

      {:error, :tracker_unavailable} ->
        error_response(conn, 503, "tracker_unavailable", "Current issue tier could not be verified; the hold remains active")

      {:error, :progress_state_unavailable} ->
        error_response(conn, 503, "progress_state_unavailable", "Durable progress state is unavailable")

      {:error, :loopback_only} ->
        error_response(conn, 403, "loopback_only", "Control endpoints are available only on loopback")

      {:error, :control_token_not_configured} ->
        error_response(conn, 503, "control_token_not_configured", "Control token is not configured")

      {:error, :invalid_control_token} ->
        error_response(conn, 401, "invalid_control_token", "Invalid control token")
    end
  end

  @spec progress(Conn.t(), map()) :: Conn.t()
  def progress(conn, %{"issue_identifier" => issue_identifier} = params) do
    attributes =
      Map.take(params, ["fingerprint", "owner_session", "progress_kind", "progress_receipt"])

    if issue_signature_supplied?(conn) do
      issue_capability_transition(conn, issue_identifier, params, :progress, fn nonce ->
        Presenter.progress_payload(
          issue_identifier,
          Map.merge(attributes, %{
            issue_request_authorized: true,
            issue_capability_nonce: nonce
          }),
          orchestrator()
        )
      end)
    else
      controlled_transition(conn, fn ->
        Presenter.progress_payload(
          issue_identifier,
          Map.put(attributes, :owner_session_authorized, true),
          orchestrator()
        )
      end)
    end
  end

  @spec review(Conn.t(), map()) :: Conn.t()
  def review(conn, %{"issue_identifier" => issue_identifier} = params) do
    attributes =
      Map.take(params, [
        "kind",
        "owner_session",
        "review_fingerprint",
        "requested_head",
        "observed_local_head",
        "observed_remote_head",
        "human_override"
      ])

    if issue_signature_supplied?(conn) do
      issue_capability_transition(conn, issue_identifier, params, :review, fn nonce ->
        Presenter.review_payload(
          issue_identifier,
          Map.merge(attributes, %{
            issue_request_authorized: true,
            issue_capability_nonce: nonce
          }),
          orchestrator()
        )
      end)
    else
      controlled_transition(conn, fn ->
        Presenter.review_payload(
          issue_identifier,
          Map.put(attributes, :owner_session_authorized, true),
          orchestrator()
        )
      end)
    end
  end

  @spec wait(Conn.t(), map()) :: Conn.t()
  def wait(conn, %{"issue_identifier" => issue_identifier} = params) do
    attributes =
      Map.take(params, [
        "expected_head",
        "receipt_path",
        "timeout_seconds",
        "waiter_script",
        "waiter_args"
      ])

    controlled_transition(conn, fn -> Presenter.wait_payload(issue_identifier, attributes, orchestrator()) end)
  end

  @spec method_not_allowed(Conn.t(), map()) :: Conn.t()
  def method_not_allowed(conn, _params) do
    error_response(conn, 405, "method_not_allowed", "Method not allowed")
  end

  @spec not_found(Conn.t(), map()) :: Conn.t()
  def not_found(conn, _params) do
    error_response(conn, 404, "not_found", "Route not found")
  end

  defp error_response(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{error: %{code: code, message: message}})
  end

  defp controlled_transition(conn, transition) do
    with :ok <- require_control_access(conn),
         {:ok, payload} <- transition.() do
      json(conn, payload)
    else
      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")

      {:error, :progress_state_unavailable} ->
        error_response(conn, 503, "progress_state_unavailable", "Durable progress state is unavailable")

      {:error, :loopback_only} ->
        error_response(conn, 403, "loopback_only", "Control endpoints are available only on loopback")

      {:error, :control_token_not_configured} ->
        error_response(conn, 503, "control_token_not_configured", "Control token is not configured")

      {:error, :invalid_control_token} ->
        error_response(conn, 401, "invalid_control_token", "Invalid control token")

      {:error, reason} ->
        error_response(conn, 422, Atom.to_string(reason), "Progress transition rejected: #{reason}")
    end
  end

  defp issue_capability_transition(conn, issue_identifier, params, kind, transition) do
    owner_session = Map.get(params, "owner_session")
    restart_id = Map.get(params, "restart_id")
    nonce = Map.get(params, "capability_nonce")
    signed_payload = Map.drop(params, ["issue_identifier"])

    with :ok <- require_loopback(conn),
         {:ok, signature} <- issue_signature_header(conn),
         :ok <-
           IssueCapability.verify_request(
             kind,
             signature,
             issue_identifier,
             owner_session,
             restart_id,
             nonce,
             signed_payload
           ),
         {:ok, payload} <- transition.(nonce),
         {:ok, signature} <-
           IssueCapability.sign_response(kind, nonce, issue_identifier, owner_session, payload) do
      json(
        conn,
        Map.merge(payload, %{
          issue_capability_nonce: nonce,
          issue_capability_signature: signature
        })
      )
    else
      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")

      {:error, reason} when reason in [:issue_not_running, :stale_issue_session] ->
        error_response(conn, 409, Atom.to_string(reason), "Issue session is no longer active")

      {:error, :progress_state_unavailable} ->
        error_response(conn, 503, "progress_state_unavailable", "Durable progress state is unavailable")

      {:error, :loopback_only} ->
        error_response(conn, 403, "loopback_only", "Issue endpoints are available only on loopback")

      {:error, :issue_capability_not_configured} ->
        error_response(conn, 503, "issue_capability_not_configured", "Issue capability authorization is not configured")

      {:error, :replayed_issue_request} ->
        error_response(conn, 409, "replayed_issue_request", "Issue request nonce has already been consumed")

      {:error, :issue_nonce_capacity_exceeded} ->
        error_response(conn, 503, "issue_nonce_capacity_exceeded", "Issue request nonce capacity is unavailable")

      {:error, :invalid_issue_capability} ->
        error_response(conn, 401, "invalid_issue_capability", "Invalid issue capability")

      {:error, :invalid_issue_capability_response} ->
        error_response(conn, 503, "invalid_issue_capability_response", "Issue capability response could not be authenticated")

      {:error, :unavailable} ->
        error_response(conn, 503, "unavailable", "Issue control is temporarily unavailable")

      {:error, reason} ->
        error_response(conn, 422, Atom.to_string(reason), "Progress transition rejected: #{reason}")
    end
  end

  defp with_control_access(conn, request) do
    case require_control_access(conn) do
      :ok -> request.()
      {:error, :loopback_only} -> error_response(conn, 403, "loopback_only", "Control endpoints are available only on loopback")
      {:error, :control_token_not_configured} -> error_response(conn, 503, "control_token_not_configured", "Control token is not configured")
      {:error, :invalid_control_token} -> error_response(conn, 401, "invalid_control_token", "Invalid control token")
    end
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end

  defp require_loopback(%Conn{remote_ip: remote_ip})
       when remote_ip in [{127, 0, 0, 1}, {0, 0, 0, 0, 0, 0, 0, 1}],
       do: :ok

  defp require_loopback(_conn), do: {:error, :loopback_only}

  defp require_control_access(conn) do
    with :ok <- require_loopback(conn),
         {:ok, expected_token} <- control_token() do
      validate_control_token(conn, expected_token)
    end
  end

  defp issue_signature_header(conn) do
    case get_req_header(conn, @issue_signature_header) do
      [signature] when is_binary(signature) and signature != "" -> {:ok, signature}
      _ -> {:error, :invalid_issue_capability}
    end
  end

  defp issue_signature_supplied?(conn),
    do: get_req_header(conn, @issue_signature_header) != []

  defp control_token do
    ControlToken.fetch()
  end

  defp validate_control_token(conn, expected_token) do
    case get_req_header(conn, "x-symphony-control-token") do
      [provided_token] when byte_size(provided_token) == byte_size(expected_token) ->
        if Plug.Crypto.secure_compare(provided_token, expected_token),
          do: :ok,
          else: {:error, :invalid_control_token}

      _ ->
        {:error, :invalid_control_token}
    end
  end
end
