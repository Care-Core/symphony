defmodule SymphonyElixirWeb.PresenterStatusTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixirWeb.Presenter

  @running %{identifier: "CC-1", session_id: "sess-1"}
  @hold %{identifier: "CC-1", reason: "input_token_resume_pending"}
  @deferred %{identifier: "CC-1"}
  @retry %{identifier: "CC-1", attempt: 1}
  @blocked %{identifier: "CC-1"}

  test "a live session reports running even while a hold record is stored" do
    assert Presenter.issue_status_for_test(@running, nil, nil, nil, @hold) == "running"

    checkpoint_hold = %{@hold | reason: "input_token_checkpoint"}
    assert Presenter.issue_status_for_test(@running, nil, nil, nil, checkpoint_hold) == "running"
  end

  test "a parked hold without a live session still reports held" do
    assert Presenter.issue_status_for_test(nil, nil, nil, nil, @hold) == "held"
    assert Presenter.issue_status_for_test(nil, @deferred, nil, nil, @hold) == "held"
  end

  test "remaining status precedence is unchanged" do
    assert Presenter.issue_status_for_test(@running, @deferred, @retry, @blocked, nil) == "running"
    assert Presenter.issue_status_for_test(nil, @deferred, nil, nil, nil) == "deferred_wait"
    assert Presenter.issue_status_for_test(nil, nil, @retry, nil, nil) == "retrying"
    assert Presenter.issue_status_for_test(nil, nil, nil, @blocked, nil) == "blocked"
  end
end
