defmodule Triton.Monitor do
  @doc """
  Reconnect to DB if there is a db_connection disconnection.
  """
  use GenServer

  @heartbeat_interval 2500

  def start_link(conn, keyspace) do
    GenServer.start_link(__MODULE__, [conn: conn, keyspace: keyspace], name: Module.concat([__MODULE__, conn]))
  end

  def init(conn: conn, keyspace: keyspace) do
    Process.send_after(self(), :restart, 500)
    Process.send_after(self(), :tick, @heartbeat_interval)
    {:ok, {conn, keyspace}}
  end

  def handle_info(:tick, {conn, keyspace} = state) do
    Xandra.execute(conn, "USE #{keyspace}", [], [pool: Xandra.Cluster])
    Process.send_after(self(), :tick, @heartbeat_interval)
    {:noreply, state}
  end

  def handle_info(:restart, {conn, _} = state) do
    Supervisor.terminate_child(Triton.Supervisor, conn)
    Supervisor.restart_child(Triton.Supervisor, conn)
    {:noreply, state}
  end
end
