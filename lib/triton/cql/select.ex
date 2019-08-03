defmodule Triton.CQL.Select do
  def build(query) do
    schema = query[:__schema__].__fields__

    select_s = select(query[:select], query[:count], query[:__table__], schema)
    where_s = where(query[:where])
    order_by_s = order_by(query[:order_by] && List.first(query[:order_by]))
    limit_s = limit(query[:limit])
    filter_s = allow_filtering(query[:allow_filtering])

    (select_s <> where_s <> order_by_s <> limit_s <> filter_s)
  end

  defp select(_, count, table, _) when count == true, do: "SELECT COUNT(*) FROM #{table}"

  defp select(fields, _, table, schema) when is_list(fields) do
    schema_fields = schema |> Enum.into(MapSet.new(), fn {k, _} -> to_string(k) end)
    req_fields = fields |> Enum.into(MapSet.new(), &to_string/1)
    filtered_fields = MapSet.intersection(req_fields, schema_fields) |> Enum.join(", ")

    "SELECT " <> filtered_fields <> " FROM #{table}"
  end

  defp select(_, _, table, _), do: "SELECT * FROM #{table}"

  defp where(fragments) when is_list(fragments) do
      " WHERE " <>
        (fragments
         |> Enum.flat_map(&where_fragment/1)
         |> Enum.join(" AND "))
  end

  defp where(_), do: ""

  defp where_fragment({k, v}) when is_list(v),
    do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}) end)

  defp where_fragment({k, v}), do: ["#{k} = " <> value(v)]

  defp where_fragment({k, :in, v}),
    do: ["#{k} IN ( " <> Enum.map_join(v, ", ", fn v -> value(v) end) <> ")"]

  defp where_fragment({k, c, v}), do: ["#{k} #{c} " <> value(v)]

  defp order_by({field, direction}), do: " ORDER BY #{field} #{direction}"
  defp order_by(_), do: ""

  defp limit(limit) when is_integer(limit), do: " LIMIT #{limit}"
  defp limit(limit) when is_atom(limit) and not is_nil(limit), do: " LIMIT :#{limit}"
  defp limit(_), do: ""

  defp allow_filtering(true), do: " ALLOW FILTERING"
  defp allow_filtering(_), do: ""

  defp value(v) when is_binary(v), do: "'" <> v <> "'"
  defp value(v) when is_boolean(v), do: to_string(v)
  defp value(v) when is_atom(v), do: ":#{v}"
  defp value(v), do: to_string(v)
end
