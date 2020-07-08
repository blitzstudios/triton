defmodule Triton.MaterializedView do
  defmacro __using__(_) do
    quote do
      import Triton.MaterializedView
      use Triton.Executor
    end
  end

  # %Triton.CQL.Select.Tests.TestView.MaterializedView{
  #   __fields__: [:id2],
  #   __from__: Triton.CQL.Select.Tests.TestTable,
  #   __name__: :test_mv,
  #   __partition_key__: [:id2]
  # }
  # %Triton.CQL.Select.Tests.TestView.Metadata{
  #   __from_metadata__: Triton.CQL.Select.Tests.TestTable.Metadata,
  #   __schema__: Triton.CQL.Select.Tests.TestView.MaterializedView,
  #   __schema_module__: Triton.CQL.Select.Tests.TestView,
  #   __table__: :test_mv,
  #   __type__: :materialized_view
  # }

  defmacro materialized_view(name, [from: from], [do: block]) do
    quote do
      outer = __MODULE__

      defmodule Metadata do
        @metadata []
        Module.put_attribute(__MODULE__, :metadata, [
          { :__type__, :materialized_view },
          { :__table__, unquote(name) },
          { :__from_metadata__, Module.concat(unquote(from), "Metadata")},
          { :__schema_module__, outer },
          { :__schema__, Module.concat(outer, "MaterializedView")}
        ])
        defstruct Module.get_attribute(__MODULE__, :metadata)
      end

      defmodule MaterializedView do
        @after_compile __MODULE__

        @materialized_view []

        unquote(block)

        Module.put_attribute(__MODULE__, :materialized_view, [
          { :__from__, unquote(from) },
          { :__name__, unquote(name) }
          | Module.get_attribute(__MODULE__, :materialized_view)
        ])

        def __after_compile__(_, _) do
          case Triton.Configuration.disable_compilation_migrations?() do
            true -> :noop
            false -> Triton.Setup.MaterializedView.setup(unquote(__CALLER__.module))
          end
        end

        defstruct Module.get_attribute(__MODULE__, :materialized_view)
      end
    end
  end

  defmacro fields(fields) do
    quote do
      Module.put_attribute(__MODULE__, :materialized_view, [
        { :__fields__, unquote(fields) }
        | Module.get_attribute(__MODULE__, :materialized_view)
      ])
    end
  end

  defmacro partition_key(keys) do
    quote do
      Module.put_attribute(__MODULE__, :materialized_view, [
        { :__partition_key__, unquote(keys) }
        | Module.get_attribute(__MODULE__, :materialized_view)
      ])
    end
  end

  defmacro cluster_columns(cols) do
    quote do
      Module.put_attribute(__MODULE__, :materialized_view, [
        { :__cluster_columns__, unquote(cols) }
        | Module.get_attribute(__MODULE__, :materialized_view)
      ])
    end
  end

  defmacro with_options(opts \\ []) do
    quote do
      Module.put_attribute(__MODULE__, :materialized_view, [
        { :__with_options__, unquote(opts) }
        | Module.get_attribute(__MODULE__, :materialized_view)
      ])
    end
  end
end
