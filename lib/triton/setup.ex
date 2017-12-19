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
      node = Application.get_env(:triton, :clusters) |> Enum.find(&(&1[:conn] == block[:__schema__].__keyspace__.__struct__.__conn__))

      try do
        {:ok, conn} = Xandra.start_link(nodes: [node[:nodes] |> Enum.random])
        Xandra.execute!(conn, "USE #{node[:keyspace]};", _params = [])
        Xandra.execute!(conn, statement, _params = [])
      rescue
        err -> IO.inspect(err)
      end
    end
  end
end
