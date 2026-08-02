defmodule SymphonyElixir.Orchestrator.ControllerTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest

  alias SymphonyElixir.ControlToken

  @endpoint SymphonyElixirWeb.Endpoint

  setup do
    endpoint_config = Application.get_env(:symphony_elixir, @endpoint, [])
    control_token = ControlToken.fetch()
    service_secret = System.get_env("SYMPHONY_WAITER_SECRET")
    :ok = ControlToken.replace_for_test(nil)

    on_exit(fn ->
      Application.put_env(:symphony_elixir, @endpoint, endpoint_config)
      restore_control_token(control_token)
      restore_env("SYMPHONY_WAITER_SECRET", service_secret)
    end)

    :ok
  end

  test "the authenticated wait route rejects issue-selected executables without launching them" do
    marker =
      Path.join(
        System.tmp_dir!(),
        "symphony-denied-waiter-#{System.unique_integer([:positive])}.txt"
      )

    script = marker <> ".sh"
    File.write!(script, "#!/bin/sh\nprintf '%s' \"$SYMPHONY_WAITER_SECRET\" > \"$1\"\n")
    File.chmod!(script, 0o700)

    on_exit(fn ->
      File.rm(marker)
      File.rm(script)
    end)

    :ok = ControlToken.replace_for_test("test-control-token")

    System.put_env(
      "SYMPHONY_WAITER_SECRET",
      "must-not-cross-controller-boundary"
    )

    endpoint_options = [
      server: false,
      secret_key_base: String.duplicate("s", 64),
      orchestrator: :unused_waiter_orchestrator
    ]

    start_supervised!({SymphonyElixirWeb.Endpoint, endpoint_options})

    response =
      build_conn()
      |> Plug.Conn.put_req_header("x-symphony-control-token", "test-control-token")
      |> post("/api/v1/MT-DENIED/wait", %{
        "expected_head" => "0123456789abcdef0123456789abcdef01234567",
        "receipt_path" => marker <> ".jsonl",
        "timeout_seconds" => 1_200,
        "waiter_script" => script,
        "waiter_args" => [marker]
      })
      |> json_response(422)

    assert response == %{
             "error" => %{
               "code" => "deferred_wait_disabled",
               "message" => "Progress transition rejected: deferred_wait_disabled"
             }
           }

    refute File.exists?(marker)
  end

  defp restore_control_token({:ok, token}), do: ControlToken.replace_for_test(token)

  defp restore_control_token({:error, :control_token_not_configured}),
    do: ControlToken.replace_for_test(nil)
end
