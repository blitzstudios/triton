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
    do:
      " SET " <>
        Enum.map_join(assignments, ", ", fn {k, v} ->
          "#{k} = " <> field_value(v, schema[k][:type])
        end)

  defp where(fragments, schema) when is_list(fragments),
    do:
      " WHERE " <>
        (fragments
         |> Enum.flat_map(fn fragment -> where_fragment(fragment, schema) end)
         |> Enum.join(" AND "))

  defp where(_, _), do: ""

  defp where_fragment({k, v}, schema) when is_list(v),
    do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}, schema) end)

  defp where_fragment({k, v}, schema), do: ["#{k} = " <> field_value(v, schema[k][:type])]

  defp where_fragment({k, :in, v}, schema),
    do: "#{k} IN (" <> Enum.map_join(v, ", ", fn v -> field_value(v, schema[k][:type]) end) <> ")"

  defp where_fragment({k, c, v}, schema), do: "#{k} #{c} " <> field_value(v, schema[k][:type])

  defp constrain(constraints, schema) when is_list(constraints),
    do:
      " IF " <>
        Enum.map_join(constraints, " AND ", fn {k, v} ->
          "#{k} = " <> field_value(v, schema[k][:type])
        end)

  defp constrain(_, _), do: ""
end
