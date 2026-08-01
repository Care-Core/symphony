defmodule Mix.Tasks.Symphony.Build do
  use Mix.Task

  @shortdoc "Build the Symphony escript with exact source provenance"

  @impl Mix.Task
  def run(_args) do
    source_sha = source_sha!()

    Mix.Task.run("compile")
    write_build_info!(source_sha)
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

  defp write_build_info!(source_sha) do
    generated_root = Path.join(Mix.Project.manifest_path(), "generated")
    generated_source = Path.join(generated_root, "symphony_build_info.ex")
    File.mkdir_p!(generated_root)

    File.write!(
      generated_source,
      """
      defmodule SymphonyElixir.BuildInfo do
        @moduledoc false
        @source_sha #{inspect(source_sha)}

        @spec source_sha() :: String.t()
        def source_sha, do: @source_sha
      end
      """
    )

    :code.purge(SymphonyElixir.BuildInfo)
    :code.delete(SymphonyElixir.BuildInfo)

    case Kernel.ParallelCompiler.compile_to_path(
           [generated_source],
           Mix.Project.compile_path(),
           return_diagnostics: true
         ) do
      {:ok, _modules, _warnings} -> :ok
      {:error, errors, _warnings} -> Mix.raise("Unable to embed build provenance: #{inspect(errors)}")
    end
  end
end
