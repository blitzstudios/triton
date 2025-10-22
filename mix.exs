defmodule Triton.Mixfile do
  use Mix.Project

  @version "0.2.4"
  @url "https://github.com/blitzstudios/triton"
  @maintainers ["Weixi Yen"]

  def project do
    [name: "Triton",
     app: :triton,
     version: @version,
     source_url: @url,
     elixir: "~> 1.4",
     description: "Pure Elixir Cassandra ORM built on top of Xandra.",
     config: "config/config.exs",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     package: package(),
     homepage_url: @url,
     deps: deps()]
  end

  def application do
    [extra_applications: [:logger],
     mod: {Triton, []}]
  end

  defp deps do
    [
      {:decimal, "~> 1.0"},
      # {:xandra, git: "https://github.com/blitzstudios/xandra", ref: "06f24c7026ec280da1d3fb40faec10487288afaa"},
      {:xandra, path: "/Users/victor/xandra_fork/xandra"},
      {:vex, "~> 0.9.1"},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:rustler, "~> 0.34.0"}
    ]
  end

  defp package do
    [
      maintainers: @maintainers,
      licenses: ["MIT"],
      links: %{github: @url},
      files: ~w(lib) ++ ~w(CHANGELOG.md LICENSE mix.exs README.md)
    ]
  end
end
