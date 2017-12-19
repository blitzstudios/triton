defmodule Triton.Table do
  defmacro __using__(_) do
    quote do
      import Triton.Table
      use Triton.Executor
    end
  end

  defmacro table(name, [keyspace: keyspace], [do: block]) do
    quote do
      outer = __MODULE__

      defmodule Metadata do
        @metadata []

        Module.put_attribute(__MODULE__, :metadata, [
          { :__table__, unquote(name) },
          { :__schema_module__, outer }
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
          { :__name__, unquote(name) },
          { :__fields__, Module.get_attribute(__MODULE__, :fields) }
          | Module.get_attribute(__MODULE__, :table)
        ])

        def __after_compile__(_, _), do: Triton.Setup.Table.setup(__MODULE__.__struct__)

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
