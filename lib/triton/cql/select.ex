defmodule Triton.CQL.Select do
  def build(query) do
    schema = query[:__schema__].__fields__

    select(query[:select], query[:count], query[:__table__], schema) <>
    where(query[:where]) <>
    order_by(query[:order_by] && List.first(query[:order_by])) <>
    limit(query[:limit])
  end

  defp select(_, count, table, _) when count === true, do: "SELECT COUNT(*) FROM #{table}"
  defp select(fields, _, table, schema) when is_list(fields) do
    schema_fields = schema |> Enum.map(fn {k, _} -> "#{k}" end)
    req_fields = fields |> Enum.map(fn k -> "#{k}" end)
    filtered_fields = MapSet.intersection(MapSet.new(req_fields), MapSet.new(schema_fields)) |> Enum.into([])
    "SELECT #{Enum.join(filtered_fields, ", ")} FROM #{table}"
  end
  defp select(_, _, table, _), do: "SELECT * FROM #{table}"

  defp where(fragments) when is_list(fragments), do: " WHERE " <> (fragments |> Enum.flat_map(fn fragment -> where_fragment(fragment) end) |> Enum.join(" AND "))
  defp where(_), do: ""
  defp where_fragment({k, v}) when is_list(v), do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}) end)
  defp where_fragment({k, v}), do: ["#{k} = #{value(v)}"]
  defp where_fragment({k, :in, v}), do: "#{k} IN (#{v |> Enum.map(fn v -> value(v) end) |> Enum.join(", ")})"
  defp where_fragment({k, c, v}), do: "#{k} #{c} #{value(v)}"

  defp order_by({field, direction}), do: " ORDER BY #{field} #{direction}"
  defp order_by(_), do: ""

  defp limit(limit) when is_integer(limit), do: " LIMIT #{limit}"
  defp limit(limit) when is_atom(limit) and not is_nil(limit), do: " LIMIT :#{limit}"
  defp limit(_), do: ""

  defp value(v) when is_binary(v), do: "'#{v}'"
  defp value(v) when is_boolean(v), do: "#{v}"
  defp value(v) when is_atom(v), do: ":#{v}"
  defp value(v), do: v
end
