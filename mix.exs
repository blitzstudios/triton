defmodule Triton.Mixfile do
  use Mix.Project

  def project do
    [app: :triton,
     version: "0.0.1",
     build_path: "../../_build",
     config_path: "../../config/config.exs",
     deps_path: "../../deps",
     lockfile: "../../mix.lock",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [extra_applications: [:logger],
     mod: {Triton, []}]
  end

  defp deps do
    [
      {:xandra, "~> 0.7.1"},
      {:poolboy, "~> 1.5"},
      {:vex, "~> 0.6"}
    ]
  end
end
