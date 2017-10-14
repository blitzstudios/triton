defmodule Triton.Setup do
  defmacro __using__(_) do
    quote do
      import Triton.Setup
    end
  end

  defmacro setup([do: block]) do
    quote do
      statement = unquote(block)
      |> Triton.CQL.Insert.build

      try do
        {:ok, conn} = Xandra.start_link(nodes: [Application.get_env(:triton, :xandra)[:nodes] |> Enum.random])
        Xandra.execute!(conn, "USE #{Application.get_env(:triton, :xandra)[:keyspace]};", _params = [])
        Xandra.execute!(conn, statement, _params = [])
      rescue
        err -> IO.inspect(err)
      end
    end
  end
end
