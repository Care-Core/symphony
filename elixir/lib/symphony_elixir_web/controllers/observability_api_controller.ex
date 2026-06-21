defmodule SymphonyElixirWeb.ObservabilityApiController do
  @moduledoc """
  JSON API for Symphony observability data.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixirWeb.{Endpoint, Presenter}

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    json(conn, Presenter.state_payload(orchestrator(), snapshot_timeout_ms()))
  end

  @spec issue(Conn.t(), map()) :: Conn.t()
  def issue(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, reason} ->
        issue_error_response(conn, reason)
    end
  end

  @spec run(Conn.t(), map()) :: Conn.t()
  def run(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.run_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, reason} ->
        issue_error_response(conn, reason)
    end
  end

  @spec events(Conn.t(), map()) :: Conn.t()
  def events(conn, %{"issue_identifier" => issue_identifier} = params) do
    case Presenter.events_payload(issue_identifier, params, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, reason} ->
        issue_error_response(conn, reason)
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

  defp issue_error_response(conn, :issue_not_found),
    do: error_response(conn, 404, "issue_not_found", "Issue not found")

  defp issue_error_response(conn, :snapshot_timeout),
    do: error_response(conn, 504, "snapshot_timeout", "Snapshot timed out")

  defp issue_error_response(conn, :snapshot_unavailable),
    do: error_response(conn, 503, "snapshot_unavailable", "Snapshot unavailable")

  defp issue_error_response(conn, _reason),
    do: error_response(conn, 500, "unknown_error", "Unknown error")

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end
end
