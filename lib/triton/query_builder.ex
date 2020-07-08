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
      false ->
        [
          {:__table__, Triton.Metadata.table(module)},
          {:__schema_module__, module}
        ]
      true -> module
    end
  end
end
