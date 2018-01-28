defmodule Mix.Tasks.Triton.Gen.KeyspaceTest do
  use ExUnit.Case

  import Support.FileHelpers
  import Mix.Tasks.Triton.Gen.Keyspace, only: [run: 1]

  test "generates a new keyspace" do
    in_tmp(fn _ ->
      run(["-k", "Schema.Keyspace", "-n", "my_keyspace"])

      assert_file("lib/schema/keyspace.ex", """
      defmodule Schema.Keyspace do
        use Triton.Keyspace

        keyspace :my_keyspace, conn: Triton.Conn do
          with_options [
            replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
          ]
        end
      end
      """)

      assert_file("config/config.exs", """
      use Mix.Config

      config :triton,
        clusters: [
          [
            conn: Triton.Conn,
            nodes: ["127.0.0.1"],
            pool: Xandra.Cluster,
            underlying_pool: DBConnection.Poolboy,
            pool_size: 10,
            keyspace: "my_keyspace"
          ]
        ]
      """)
    end)
  end

  test "generates a new keyspace with existing config file" do
    in_tmp(fn _ ->
      File.mkdir_p!("config")

      File.write!("config/config.exs", """
      # Hello
      use Mix.Config
      # World
      """)

      run(["-k", "Schema.Keyspace", "-n", "my_keyspace"])

      assert_file("config/config.exs", """
      # Hello
      use Mix.Config

      config :triton,
        clusters: [
          [
            conn: Triton.Conn,
            nodes: ["127.0.0.1"],
            pool: Xandra.Cluster,
            underlying_pool: DBConnection.Poolboy,
            pool_size: 10,
            keyspace: "my_keyspace"
          ]
        ]

      # World
      """)
    end)
  end
end
