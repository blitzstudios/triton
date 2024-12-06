defmodule Triton.Setup.DbConnection do
  def get_cluster(config) do
    # nodes = Enum.shuffle(config[:nodes] || [])
    nodes = config[:nodes] || []

    node_config = Keyword.merge(config, [
      name: __MODULE__,
      nodes: nodes,
      # backoff_type: :stop,
      backoff_max: 5_000,
      # connect_timeout: 2_000,
      # refresh_topology_interval: 10_000,
      queue_checkouts_before_connecting: [
        max_size: 1000,
        timeout: 50_000
      ]
    ])
    |> Keyword.drop([:autodiscovery])

    case Xandra.Cluster.start_link(node_config) do
      {:ok, cluster} ->
        IO.inspect(cluster, label: "new connection")
        IO.inspect(nodes, label: "nodes")
        test_connection(cluster)
        |> IO.inspect(label: "test_connection")
        {:ok, cluster}

      {:error, {:already_started, cluster}} ->
        IO.inspect(cluster, label: "already started")
        {:ok, cluster}

      {:error, _reason} = err ->
        {:error, :connection_failed}
    end
  end

  defp test_connection(cluster) do
    Xandra.Cluster.execute!(cluster, "SELECT now() FROM system.local", _params = [])
  end
end
