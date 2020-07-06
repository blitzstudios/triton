defmodule Triton.Keyspace do
  defmacro __using__(_) do
    quote do
      import Triton.Keyspace
    end
  end

  defmacro keyspace(name, [conn: conn], [do: block]) do
    quote do
      @after_compile __MODULE__

      @keyspace []
      @fields %{}

      unquote(block)

      Module.put_attribute(__MODULE__, :keyspace, [
        { :__conn__, unquote(conn) },
        { :__name__, unquote(name) }
        | Module.get_attribute(__MODULE__, :keyspace)
      ])

      def __after_compile__(_, _) do
        case Triton.Configuration.disable_compilation_migrations?() do
          true -> :noop
          false -> Triton.Setup.Keyspace.setup(__MODULE__.__struct__)
        end
      end

      defstruct Module.get_attribute(__MODULE__, :keyspace)
    end
  end

  defmacro with_options(opts) do
    quote do
      Module.put_attribute(__MODULE__, :keyspace, [
        { :__with_options__, unquote(opts) }
        | Module.get_attribute(__MODULE__, :keyspace)
      ])
    end
  end
end
