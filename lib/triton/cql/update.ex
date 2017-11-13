defmodule Triton.CQL.Update do
  def build(query) do
    schema = query[:__schema__].__fields__

    update(query[:__table__]) <>
    set(query[:update], schema) <>
    where(query[:where], schema) <>
    constrain(query[:constrain], schema) <>
    if_exists(query[:if_exists])
  end

  defp update(table), do: "UPDATE #{table}"
  defp set(assignments, schema) when is_list(assignments), do: " SET #{assignments |> Enum.map(fn {k, v} -> "#{k} = #{value(v, schema[k][:type])}" end) |> Enum.join(", ")}"

  defp where(fragments, schema) when is_list(fragments), do: " WHERE " <> (fragments |> Enum.flat_map(fn fragment -> where_fragment(fragment, schema) end) |> Enum.join(" AND "))
  defp where(_, _), do: ""
  defp where_fragment({k, v}, schema) when is_list(v), do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}, schema) end)
  defp where_fragment({k, v}, schema), do: ["#{k} = #{value(v, schema[k][:type])}"]
  defp where_fragment({k, :in, v}, schema), do: "#{k} IN (#{v |> Enum.map(fn v -> value(v, schema[k][:type]) end) |> Enum.join(", ")})"
  defp where_fragment({k, c, v}, schema), do: "#{k} #{c} #{value(v, schema[k][:type])}"

  if Mix.env == :prod do
    defp constrain(constraints, schema) when is_list(constraints), do: " IF #{constraints |> Enum.map(fn {k, v} -> "#{k} = #{value(v, schema[k][:type])}" end) |> Enum.join(" AND ")}"
  end
  defp constrain(_, _), do: ""

  if Mix.env == :prod do
    defp if_exists(flag) when flag == true, do: " IF EXISTS"
  end
  defp if_exists(_), do: ""

  defp value(nil, _), do: "NULL"
  defp value(v, {_, _}), do: v
  defp value(v, _) when is_boolean(v), do: "#{v}"
  defp value(v, _) when is_binary(v), do: binary_value(v)
  defp value(v, _) when is_atom(v), do: ":#{v}"
  defp value(%DateTime{} = d, _), do: DateTime.to_unix(d)
  defp value(v, _), do: v

  defp binary_value(v) do
    cond do
      String.valid?(v) && String.contains?(v, ["'", "\""]) -> "$$#{v}$$"
      true -> "'#{v}'"
    end
  end
end
