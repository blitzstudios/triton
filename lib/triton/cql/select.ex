defmodule Triton.CQL.Select do
  def build(query) do
    schema = query[:__schema__].__fields__

    [
      select(query[:select], query[:count], query[:__table__], schema),
      where(query[:where]),
      order_by(query[:order_by] && List.first(query[:order_by])),
      limit(query[:limit]),
      allow_filtering(query[:allow_filtering])
    ]
    |> IO.iodata_to_binary
  end

  defp select(_, count, table, _) when count === true, do: ["SELECT COUNT(*) FROM ", to_string(table)]
  defp select(fields, _, table, schema) when is_list(fields) do
    schema_fields = schema |> Enum.map(fn {k, _} -> to_string(k) end)
    req_fields = fields |> Enum.map(fn k -> to_string(k) end)
    filtered_fields =
      MapSet.intersection(
        MapSet.new(req_fields),
        MapSet.new(schema_fields))
      |> Enum.reduce(:first, fn
           field, :first -> [field]
           field, acc -> [acc, ", ", field]
         end)
    ["SELECT ", filtered_fields, " FROM ", to_string(table)]
  end
  defp select(_, _, table, _), do: ["SELECT * FROM ", to_string(table)]

  defp where(fragments) when is_list(fragments) do
    [
      " WHERE ",
      fragments
      |> Enum.reduce(:first, fn
           frag, :first -> [where_fragment(frag)]
           frag, acc -> [acc, " AND ", where_fragment(frag)]
         end)
    ]
  end
  defp where(_), do: []
  defp where_fragment({k, v}) when is_list(v), do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}) end)
  defp where_fragment({k, v}), do: [to_string(k), " = ", value(v)]
  defp where_fragment({k, :in, v}) do
    [
      to_string(k),
      " IN (",
      v
      |> Enum.reduce(:first, fn
           v, :first -> [value(v)]
           v, acc -> [acc, ", ", value(v)]
         end),
      ")"
    ]
  end
  defp where_fragment({k, c, v}), do: [to_string(k), " ", to_string(c), " ",  value(v)]

  defp order_by({field, direction}), do: [" ORDER BY ", to_string(field), " ", to_string(direction)]
  defp order_by(_), do: []

  defp limit(limit) when is_integer(limit), do: [" LIMIT ", to_string(limit)]
  # TODO: What does this do?
  defp limit(limit) when is_atom(limit) and not is_nil(limit), do: [" LIMIT :", to_string(limit)]
  defp limit(_), do: []

  defp allow_filtering(true), do: [" ALLOW FILTERING"]
  defp allow_filtering(_), do: []

  defp value(v), do: [Triton.CQL.Encode.encode(v) |> to_string]
end
