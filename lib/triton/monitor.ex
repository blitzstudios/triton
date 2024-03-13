defmodule Triton.Monitor do
  @moduledoc """
  Reconnect to DB if there is a db_connection disconnection.
  """
  use GenServer

  @default_config [
    health_check_delay: 5000,
    health_check_interval: 1000
  ]

  def start_link([conn: conn, keyspace: keyspace, delay: delay, interval: interval]) do
    start_link(conn, keyspace, delay, interval)
  end

  def start_link(conn, keyspace, delay, interval) do
    GenServer.start_link(__MODULE__, [
      conn: conn,
      keyspace: keyspace,
      delay: delay || @default_config[:health_check_delay],
      interval: interval || @default_config[:health_check_interval]
    ], name: Module.concat([__MODULE__, conn]))
  end

  def init(conn: conn, keyspace: keyspace, delay: delay, interval: interval) do
    Process.send_after(self(), :tick, delay)
    {:ok, {conn, keyspace, interval}}
  end

  def handle_info(:tick, {conn, keyspace, interval} = state) do
    Xandra.Cluster.execute(conn, "USE #{keyspace}")
    Process.send_after(self(), :tick, interval)
    {:noreply, state}
  end
end
