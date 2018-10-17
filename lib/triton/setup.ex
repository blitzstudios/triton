defmodule Triton.Setup do
  defmacro __using__(_) do
    quote do
      import Triton.Setup
    end
  end

  defmacro setup([do: block]) do
    quote do
      block = unquote(block)
      statement = Triton.CQL.Insert.build(block)

      node_config =
        Application.get_env(:triton, :clusters)
        |> Enum.find(&(&1[:conn] == block[:__schema__].__keyspace__.__struct__.__conn__))
        |> Keyword.take([:nodes, :authentication, :keyspace])

      node_config = Keyword.put(node_config, :nodes, [node_config[:nodes] |> Enum.random()])
      try do
        {:ok, conn} = Xandra.start_link(node_config)
        Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
        Xandra.execute!(conn, statement, _params = [])
      rescue
        err -> IO.inspect(err)
      end
    end
  end
end
