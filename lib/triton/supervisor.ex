defmodule Triton.Supervisor do
  @moduledoc """
  There is one supervisor per cluster, which is composed of a Xandra worker + Triton monitor.
  The Supervisor uses a one_for_all strategy to ensure that clusters are restarted when the monitor fails.

  This design is necessary because when Xandra / DBConnection receives a disconnection error it
  cannot be restarted without terminating & restarting Xandra.

  Separation of clusters ensure that one cluster going down doesn't affect the other.
  """
  use Supervisor

  def start_link(config), do: Supervisor.start_link(__MODULE__, config, name: Module.concat([__MODULE__, config[:conn]]))

  def init(config) do
    children = [
      worker(Xandra.Cluster, [[
        {:name, config[:conn]},
        {:after_connect, fn(conn) -> Xandra.execute(conn, "USE #{config[:keyspace]}") end}
        | config
      ]], [id: config[:conn]]),

      worker(Triton.Monitor, [
        config[:conn],
        config[:keyspace],
        config[:health_check_delay],
        config[:health_check_interval]
      ], [id: make_ref()])
    ]

    supervise(children, strategy: :one_for_all)
  end
end
