defmodule Triton.CQL.Update do
  import Triton.CQL.Helper

  def build(query) do
    schema = query[:__schema__].__fields__

    update(query[:__table__]) <>
      set(query[:update], schema) <>
      where(query[:where], schema) <>
      constrain(query[:constrain], schema) <> if_exists(query[:if_exists])
  end

  defp update(table), do: "UPDATE #{table}"

  defp set(assignments, schema) when is_list(assignments),
    do: " SET " <> Enum.map_join(assignments, ", ", &key_eq_field_value(&1, schema))

  defp where(fragments, schema) when is_list(fragments),
    do:
      " WHERE " <>
        (fragments
         |> Enum.flat_map(& where_fragment(&1, schema))
         |> Enum.join(" AND "))

  defp where(_, _), do: ""

  defp where_fragment({k, v}, schema) when is_list(v),
    do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}, schema) end)

  defp where_fragment({k, v}, schema), do: ["#{k} = " <> field_value(v, schema[k][:type])]

  defp where_fragment({k, :in, v}, schema),
    do: "#{k} IN (" <> Enum.map_join(v, ", ", fn v -> field_value(v, schema[k][:type]) end) <> ")"

  defp where_fragment({k, c, v}, schema), do: "#{k} #{c} " <> field_value(v, schema[k][:type])

  defp constrain(constraints, schema) when is_list(constraints),
    do: " IF " <> Enum.map_join(constraints, " AND ", &key_eq_field_value(&1, schema))

  defp constrain(_, _), do: ""

  defp key_eq_field_value({k, v}, schema) do
    "#{k} = " <> field_value(v, schema[k][:type])
  end
end
