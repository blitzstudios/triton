defmodule Triton.CQL.Insert do
  def build(query) do
    schema = query[:__schema__].__fields__

    insert(query[:insert], query[:__table__], schema) <> if_not_exists(query[:if_not_exists])
  end

  defp insert(fields, table, schema) when is_list(fields),
    do: "INSERT INTO #{table} (#{field_keys(fields)}) VALUES (#{field_values(fields, schema)})"

  defp field_keys(fields) when is_list(fields),
    do: fields |> Enum.map(fn {k, _} -> k end) |> Enum.join(", ")

  defp field_values(fields, schema) when is_list(fields),
    do: fields |> Enum.map(fn {k, v} -> field_value(v, schema[k][:type]) end) |> Enum.join(", ")

  defp field_value(nil, _), do: "NULL"
  defp field_value(field, {_, _}), do: field
  defp field_value(field, _) when is_boolean(field), do: "#{field}"
  defp field_value(field, _) when is_binary(field), do: binary_value(field)
  defp field_value(field, _) when is_atom(field), do: ":#{field}"
  defp field_value(%DateTime{} = d, _), do: DateTime.to_unix(d, :millisecond)
  defp field_value(field, _), do: field

  defp if_not_exists(flag) when flag == true, do: " IF NOT EXISTS"
  defp if_not_exists(_), do: ""

  defp binary_value(v) do
    cond do
      String.valid?(v) && String.contains?(v, ["'", "\""]) -> "$$#{v}$$"
      true -> "'#{v}'"
    end
  end
end
