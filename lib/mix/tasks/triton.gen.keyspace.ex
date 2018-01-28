defmodule Mix.Tasks.Triton.Gen.Keyspace do
  use Mix.Task

  import Mix.Triton
  import Mix.Generator

  @shortdoc "Generates a new keyspace"

  @moduledoc """
  Generates a new keyspace.

  The keyspace will be placed in the `lib` directory.

  ## Examples

      mix triton.gen.keyspace -k Schema.Keyspace -n my_keyspace

  This generator will automatically open the config/config.exs

  ## Command line options

    * `-k`, `--keyspace` - the keyspace to generate
    * `-n`, `--name` - the name of keyspace to create and use

  """

  @switches [keyspace: :string, name: :string]
  @aliases [k: :keyspace, n: :name]

  @doc false
  def run(argv) do
    no_umbrella!("triton.gen.keyspace")

    {keyspace, name} =
      case OptionParser.parse(argv, switches: @switches, aliases: @aliases) do
        {[keyspace: keyspace, name: name], _, _} ->
          {keyspace, name}

        _ ->
          Mix.raise(
            "triton.gen.keyspace expects the keyspace and the the name of keyspace to be given as -k Schema.Keyspace -n my_keyspace"
          )
      end

    underscored = Macro.underscore(keyspace)
    file = Path.join("lib", underscored) <> ".ex"
    opts = [module: keyspace, keyspace: name, conn: "Triton.Conn"]

    create_directory(Path.dirname(file))
    create_file(file, keyspace_template(opts))

    case File.read("config/config.exs") do
      {:ok, contents} ->
        Mix.shell().info([:green, "* updating ", :reset, "config/config.exs"])

        File.write!(
          "config/config.exs",
          String.replace(contents, "use Mix.Config", config_template(opts))
        )

      {:error, _} ->
        create_file("config/config.exs", config_template(opts))
    end
  end

  embed_template(:keyspace, """
  defmodule <%= @module %> do
    use Triton.Keyspace

    keyspace :<%= @keyspace %>, conn: <%= @conn %> do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end
  """)

  embed_template(:config, """
  use Mix.Config

  config :triton,
    clusters: [
      [
        conn: <%= @conn %>,
        nodes: ["127.0.0.1"],
        pool: Xandra.Cluster,
        underlying_pool: DBConnection.Poolboy,
        pool_size: 10,
        keyspace: "<%= @keyspace %>"
      ]
    ]
  """)
end
