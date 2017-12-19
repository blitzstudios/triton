defmodule Triton do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = for config <- Application.get_env(:triton, :clusters, []), into: [] do
      worker(Xandra, [[
        {:name, config[:conn]},
        {:after_connect, fn(conn) -> Xandra.execute(conn, "USE #{config[:keyspace]}") end}
        | config
      ]], [id: make_ref()])
    end

    opts = [strategy: :one_for_one, name: Triton.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
