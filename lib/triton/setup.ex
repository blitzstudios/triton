defmodule Triton.Setup do
  defmacro __before_compile__(_) do
    module = __CALLER__.module
    statements = Module.get_attribute(module, :setup_statements)

    quote do
      def __execute__(), do: __execute__(nil, nil)
      def __execute__(_, _) do
        unquote(Macro.escape(statements))
        |> Enum.each(fn {statement, block} ->
          try do
            node_config =
              Application.get_env(:triton, :clusters)
              |> Enum.find(&(&1[:conn] == block[:__schema__].__keyspace__.__struct__.__conn__))
              |> Keyword.take([:nodes, :authentication, :keyspace])

            node_config = Keyword.put(node_config, :nodes, [node_config[:nodes] |> Enum.random()])

            {:ok, _apps} = Application.ensure_all_started(:xandra)
            {:ok, conn} = Xandra.start_link(node_config)
            Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
            Xandra.execute!(conn, statement, _params = [])
          rescue
            err -> IO.inspect(err)
          end
        end)
      end
    end
  end

  defmacro __using__(_) do
    module = __CALLER__.module
    Module.register_attribute(module, :setup_statements, accumulate: true)

    quote do
      import Triton.Setup
      @before_compile Triton.Setup
      @after_compile {unquote(module), :__execute__}
    end
  end

  defmacro setup([do: block]) do
    quote do
      block = unquote(block)
      statement = Triton.CQL.Insert.build(block)

      Module.put_attribute(__MODULE__, :setup_statements, {statement, block})
    end
  end
end
