use Mix.Config

config :triton,
  clusters: [
    [
      conn: TritonTests.Conn,
      nodes: ["127.0.0.1", "127.0.0.2", "127.0.0.3"],
      pool_size: 10,
      keyspace: "triton_tests",
      autodiscovery: false
    ]
  ],
  disable_compilation_migrations: true
