defmodule Triton.CQL.Insert do
  import Triton.CQL.Helper

  def build(query) do
    schema = query[:__schema__].__fields__

    insert(query[:insert], query[:__table__], schema) <> if_not_exists(query[:if_not_exists])
  end

  defp insert(fields, table, schema) when is_list(fields),
    do:
      "INSERT INTO #{table} (" <>
        field_keys(fields) <> ") VALUES (" <> field_values(fields, schema) <> ")"

  defp field_keys(fields) when is_list(fields),
    do: fields |> Keyword.keys() |> Enum.join(", ")

  defp field_values(fields, schema) when is_list(fields),
    do: Enum.map_join(fields, ", ", fn {k, v} -> field_value(v, schema[k][:type]) end)
end
