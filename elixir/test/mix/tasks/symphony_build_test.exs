defmodule Mix.Tasks.Symphony.BuildTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Symphony.Build

  test "embeds the exact clean source commit in the escript" do
    Build.run([])

    expected_source_sha =
      "git"
      |> System.cmd(["rev-parse", "--verify", "HEAD^{commit}"],
        cd: Path.expand("..", File.cwd!())
      )
      |> elem(0)
      |> String.trim()

    assert {:ok, sections} = :escript.extract(~c"bin/symphony", [])
    assert {:ok, files} = :zip.extract(Keyword.fetch!(sections, :archive), [:memory])

    assert {_path, application_binary} =
             Enum.find(files, fn {path, _binary} ->
               to_string(path) == "symphony_elixir/ebin/symphony_elixir.app"
             end)

    assert {:ok, tokens, _end_location} =
             application_binary
             |> String.to_charlist()
             |> :erl_scan.string()

    assert {:ok, {:application, :symphony_elixir, properties}} = :erl_parse.parse_term(tokens)

    assert expected_source_sha ==
             properties
             |> Keyword.fetch!(:env)
             |> Keyword.fetch!(:build_source_sha)
  end
end
