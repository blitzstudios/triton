defmodule Triton do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Xandra, [[
        {:name, Triton.Conn},
        {:after_connect, fn(conn) -> Xandra.execute(conn, "USE #{Application.get_env(:triton, :xandra)[:keyspace]}") end}
        | Application.get_env(:triton, :xandra)
      ]]),
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Data.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
