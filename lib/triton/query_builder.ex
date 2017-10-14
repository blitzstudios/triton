defmodule Triton.QueryBuilder do
  def build_query(type, module, value) do
    quote do
      [
        { unquote(type), unquote(value) }
        | Triton.QueryBuilder.query_list(unquote(module))
      ]
    end
  end

  def query_list(module) do
    case is_list(module) do

      false -> [
        {:__table__, Module.concat(module, Metadata).__struct__.__table__},
        {:__schema__, Module.concat(Module.concat(module, Metadata).__struct__.__schema_module__, Table).__struct__ }
      ]
      true -> module
    end
  end
end
