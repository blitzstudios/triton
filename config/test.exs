use Mix.Config

config :triton,
  clusters: [
    [
      conn: TritonTests.Conn,
      nodes: ["127.0.0.1"],
      pool_size: 10,
      keyspace: "triton_tests",
      load_balancing: {Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin, [local_data_center: :from_first_peer]}
    ]
  ],
  disable_compilation_migrations: true
