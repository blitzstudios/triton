defmodule Triton.Mixfile do
  use Mix.Project

  @version "0.1.5"
  @url "https://github.com/blitzstudios/triton"
  @maintainers ["Weixi Yen"]

  def project do
    [name: "Triton",
     app: :triton,
     version: @version,
     source_url: @url,
     elixir: "~> 1.4",
     description: "Pure Elixir Cassandra ORM built on top of Xandra.",
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
      {:xandra, "~> 0.9"},
      {:poolboy, "~> 1.5"},
      {:vex, "~> 0.6"},
      {:ex_doc, ">= 0.0.0", only: :dev}
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
