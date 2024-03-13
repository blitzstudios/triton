defmodule Triton.Supervisor do
  require Logger
  use Supervisor

  @moduledoc """
  There is one supervisor per cluster, which is composed of a Xandra worker + Triton monitor.
  The Supervisor uses a one_for_all strategy to ensure that clusters are restarted when the monitor fails.

  This design is necessary because when Xandra / DBConnection receives a disconnection error it
  cannot be restarted without terminating & restarting Xandra.

  Separation of clusters ensure that one cluster going down doesn't affect the other.
  """

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: Module.concat([__MODULE__, config[:conn]]))
  end

  def init(config) do
    xandra_config = Keyword.put(config, :name, config[:conn])
                    |> Keyword.delete(:conn)

    Logger.debug("Using Xandra config: #{inspect(xandra_config)}")

    monitor_config = %{
      id: make_ref(),
      start: {Triton.Monitor, :start_link, [
        config[:conn],
        config[:keyspace],
        config[:health_check_delay],
        config[:health_check_interval]
      ]}
    }

    children = [
      {Xandra.Cluster, xandra_config},
      monitor_config
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
