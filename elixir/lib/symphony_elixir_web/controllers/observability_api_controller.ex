defmodule SymphonyElixirWeb.ObservabilityApiController do
  @moduledoc """
  JSON API for Symphony observability data.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixirWeb.{Endpoint, Presenter}

  @control_token_env "SYMPHONY_CONTROL_TOKEN"

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    json(conn, Presenter.state_payload(orchestrator(), snapshot_timeout_ms()))
  end

  @spec issue(Conn.t(), map()) :: Conn.t()
  def issue(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")
    end
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
  def resume(conn, %{"issue_identifier" => issue_identifier}) do
    with :ok <- require_control_access(conn),
         {:ok, payload} <- Presenter.resume_payload(issue_identifier, orchestrator()) do
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

      {:error, :loopback_only} ->
        error_response(conn, 403, "loopback_only", "Control endpoints are available only on loopback")

      {:error, :control_token_not_configured} ->
        error_response(conn, 503, "control_token_not_configured", "Control token is not configured")

      {:error, :invalid_control_token} ->
        error_response(conn, 401, "invalid_control_token", "Invalid control token")
    end
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
    case require_loopback(conn) do
      :ok ->
        with {:ok, expected_token} <- control_token() do
          validate_control_token(conn, expected_token)
        end

      error ->
        error
    end
  end

  defp control_token do
    case System.get_env(@control_token_env) do
      token when is_binary(token) and byte_size(token) > 0 -> {:ok, token}
      _ -> {:error, :control_token_not_configured}
    end
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
