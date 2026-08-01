defmodule Mix.Tasks.Symphony.Build do
  use Mix.Task

  @moduledoc """
  Builds the Symphony escript with source provenance in its application metadata.
  """
  @shortdoc "Build the Symphony escript with exact source provenance"

  @impl Mix.Task
  def run(_args) do
    source_sha = source_sha!()

    Mix.Task.run("compile")
    write_build_provenance!(source_sha)
    Mix.Task.reenable("escript.build")
    Mix.Task.run("escript.build")
  end

  defp source_sha! do
    repository_root = Path.expand("../../../..", __DIR__)

    with {source_sha, 0} <-
           System.cmd("git", ["rev-parse", "--verify", "HEAD^{commit}"],
             cd: repository_root,
             stderr_to_stdout: true
           ),
         {status, 0} <-
           System.cmd("git", ["status", "--porcelain", "--untracked-files=no"],
             cd: repository_root,
             stderr_to_stdout: true
           ),
         source_sha <- String.trim(source_sha),
         true <- Regex.match?(~r/\A[0-9a-f]{40}\z/, source_sha) do
      if String.trim(status) == "", do: source_sha, else: source_sha <> "-dirty"
    else
      _ -> Mix.raise("Symphony builds require a clean Git checkout at an exact commit")
    end
  end

  defp write_build_provenance!(source_sha) do
    app = Mix.Project.config()[:app]
    application_path = Path.join(Mix.Project.compile_path(), "#{app}.app")

    case :file.consult(String.to_charlist(application_path)) do
      {:ok, [{:application, ^app, properties}]} ->
        environment =
          properties
          |> Keyword.get(:env, [])
          |> Keyword.put(:build_source_sha, source_sha)

        application = {:application, app, Keyword.put(properties, :env, environment)}
        File.write!(application_path, :io_lib.format(~c"~tp.~n", [application]))

      other ->
        Mix.raise("Unable to embed build provenance in #{application_path}: #{inspect(other)}")
    end
  end
end
