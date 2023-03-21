defmodule Triton.Table do
  defmacro __using__(_) do
    quote do
      import Triton.Table
      use Triton.Executor
    end
  end

  # %Triton.CQL.Select.Tests.TestTable.Table{
  #   __cluster_columns__: [:id2],
  #   __dual_write_keyspace__: nil,
  #   __fields__: %{id1: %{opts: [], type: :text}, id2: %{opts: [], type: :bigint}},
  #   __keyspace__: Triton.CQL.Select.Tests.TestKeyspace,
  #   __name__: :test_table,
  #   __partition_key__: [:id]
  # }
  # %Triton.CQL.Select.Tests.TestTable.Metadata{
  #   __schema__: Triton.CQL.Select.Tests.TestTable.Table,
  #   __schema_module__: Triton.CQL.Select.Tests.TestTable,
  #   __table__: :test_table,
  #   __type__: :table
  # }

  defmacro table(name, params, [do: block]) do
    dual_write_keyspace = params[:dual_write_keyspace]
    keyspace = params[:keyspace]
    transform_streams = params[:transform_streams]

    quote do
      outer = __MODULE__

      defmodule Metadata do
        @metadata []

        Module.put_attribute(__MODULE__, :metadata, [
          { :__type__, :table },
          { :__table__, unquote(name) },
          { :__schema_module__, outer },
          { :__schema__, Module.concat(outer, Table)}
        ])
        defstruct Module.get_attribute(__MODULE__, :metadata)
      end

      defmodule Table do
        @after_compile __MODULE__

        @table []
        @fields %{}

        unquote(block)

        Module.put_attribute(__MODULE__, :table, [
          { :__keyspace__, unquote(keyspace) },
          { :__dual_write_keyspace__, unquote(dual_write_keyspace) },
          { :__name__, unquote(name) },
          { :__transform_streams__, unquote(transform_streams)},
          { :__fields__, Module.get_attribute(__MODULE__, :fields) }
          | Module.get_attribute(__MODULE__, :table)
        ])

        def __after_compile__(_, _) do
          case Triton.Configuration.disable_compilation_migrations?() do
            true -> :noop
            false -> Triton.Setup.Table.setup(unquote(__CALLER__.module))
          end
        end

        defstruct Module.get_attribute(__MODULE__, :table)
      end
    end
  end

  defmacro field(name, type, opts \\ []) do
    quote do
      fields = Module.get_attribute(__MODULE__, :fields)
        |> Map.put(unquote(name), %{
          type: unquote(type),
          opts: unquote(opts)
        })

      Module.put_attribute(__MODULE__, :fields, fields)
    end
  end

  defmacro partition_key(keys) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :__partition_key__, unquote(keys) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end

  defmacro cluster_columns(cols) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :__cluster_columns__, unquote(cols) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end

  defmacro with_options(opts \\ []) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :__with_options__, unquote(opts) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end
end
