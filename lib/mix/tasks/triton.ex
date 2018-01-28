defmodule Mix.Tasks.Triton do
  use Mix.Task

  @shortdoc "Prints Triton help information"

  @moduledoc """
  Prints Triton tasks and their information.
      mix triton
  """

  @doc false
  def run(args) do
    {_opts, args, _} = OptionParser.parse(args)

    case args do
      [] ->
        general()

      _ ->
        Mix.raise("Invalid arguments, expected: mix triton")
    end
  end

  defp general() do
    Application.ensure_all_started(:triton)
    Mix.shell().info("Triton v#{Application.spec(:triton, :vsn)}")
    Mix.shell().info("Pure Elixir Cassandra ORM built on top of Xandra.")
    Mix.shell().info("\nAvailable tasks:\n")
    Mix.Tasks.Help.run(["--search", "triton."])
  end
end
