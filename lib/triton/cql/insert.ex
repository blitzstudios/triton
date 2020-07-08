defmodule Triton.CQL.Insert do
  def build(query) do
    schema = Triton.Metadata.fields(query[:__schema_module__])

    insert(query[:insert], query[:__table__], schema) <>
    if_not_exists(query[:if_not_exists])
  end

  defp insert(fields, table, schema) when is_list(fields), do: "INSERT INTO #{table} (#{field_keys(fields)}) VALUES (#{field_values(fields, schema)})"
  defp field_keys(fields) when is_list(fields), do: fields |> Enum.map(fn {k, _} -> k end) |> Enum.join(", ")
  defp field_values(fields, schema) when is_list(fields), do: fields |> Enum.map(fn {k, v} -> field_value(v, schema[k][:type]) end) |> Enum.join(", ")
  defp field_value(v, {_collection_type, _inner_types}) when not is_nil(v) and is_atom(v), do: ":#{v}"
  defp field_value(v, {_collection_type, _inner_types}) when not is_nil(v), do: v
  defp field_value(field, _field_type), do: Triton.CQL.Encode.encode(field)

  defp if_not_exists(flag) when flag == true, do: " IF NOT EXISTS"
  defp if_not_exists(_), do: ""
end
