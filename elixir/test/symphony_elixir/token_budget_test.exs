defmodule SymphonyElixir.TokenBudgetTest do
  use SymphonyElixir.TestSupport

  test "config selects the smallest matching case-insensitive label limit" do
    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: 1_000,
      codex_input_token_limits_by_label: %{"Symphony Normal" => 800, "URGENT" => 250}
    )

    issue = %Issue{labels: [" symphony normal ", "urgent"]}
    assert Config.input_token_limit_for_issue(issue) == 250
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == 1_000

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limit: nil,
      codex_input_token_limits_by_label: %{"Symphony Normal" => 400}
    )

    assert Config.input_token_limit_for_issue(%Issue{labels: ["SYMPHONY NORMAL"]}) == 400
    assert Config.input_token_limit_for_issue(%Issue{labels: ["frontend"]}) == nil
  end

  test "config rejects non-positive limits and invalid warning or grace values" do
    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_limit: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limit"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{"symphony normal" => -1}
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_limits_by_label"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{" " => 100}
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "labels must not be blank"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_input_token_limits_by_label: %{"Symphony Normal" => 800, " symphony normal " => 400}
    )

    assert :ok = Config.validate!()
    assert Config.input_token_limit_for_issue(%Issue{labels: ["SYMPHONY NORMAL"]}) == 400

    for limits <- [
          %{"Symphony Normal" => -1, " symphony normal " => 400},
          %{"Symphony Normal" => 400, " symphony normal " => -1}
        ] do
      write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_limits_by_label: limits)
      assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
      assert message =~ "limits must be positive integers"
    end

    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_warning_ratio: 1.0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_warning_ratio"

    write_workflow_file!(Workflow.workflow_file_path(), codex_input_token_checkpoint_grace: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.input_token_checkpoint_grace"
  end
end
