use Mix.Config

config :triton,
  clusters: [
    [
      conn: TritonTests.Conn,
      nodes: ["127.0.0.1"],
      pool_size: 10,
      keyspace: "triton_tests"
    ]
  ],
  disable_compilation_migrations: true

