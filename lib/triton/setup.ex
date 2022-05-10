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
            cluster =
              Application.get_env(:triton, :clusters)
              |> Enum.find(&(&1[:conn] == Triton.Metadata.conn(block[:__schema_module__])))
            name = cluster |> Keyword.get(:conn)
            node_config = cluster |> Keyword.put(:name, name)

            {:ok, _apps} = Application.ensure_all_started(:xandra)
            {:ok, _conn} =
              case Xandra.Cluster.start_link(node_config) do
                {:ok, pid} -> {:ok, pid}
                {:error, {:already_started, pid}} -> {:ok, pid}
              end

            Xandra.Cluster.run(name, fn conn ->
              Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
              Xandra.execute!(conn, statement, _params = [])
            end)
          rescue
            err -> IO.inspect(err, label: inspect(block[:__schema_module__]))
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
