defmodule Triton do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    configs = Application.get_env(:triton, :clusters, [])

    workers = for config <- configs, into: [] do
      worker(Xandra, [[
        {:name, config[:conn]},
        {:after_connect, fn(conn) -> Xandra.execute(conn, "USE #{config[:keyspace]}") end}
        | config
      ]], [id: config[:conn]])
    end

    monitors = for config <- configs, into: [] do
      worker(Triton.Monitor, [config[:conn], config[:keyspace]], [id: make_ref()])
    end

    opts = [strategy: :one_for_one, name: Triton.Supervisor]
    Supervisor.start_link(workers ++ monitors, opts)
  end
end
