defmodule Triton.CQL.Delete do
  def build(query) do
    delete(query[:delete], query[:where], query[:__table__]) <>
    constrain(query[:constrain]) <>
    if_exists(query[:if_exists])
  end

  defp delete(fields, where, table) when is_list(fields) and is_list(where), do: "DELETE #{fields |> Enum.join(", ")} FROM #{table}#{where(where)}"
  defp delete(:all, where, table) when is_list(where), do: "DELETE FROM #{table}#{where(where)}"
  defp delete(_, _, _), do: ""

  defp where(fragments) when is_list(fragments), do: " WHERE " <> (fragments |> Enum.flat_map(fn fragment -> where_fragment(fragment) end) |> Enum.join(" AND "))
  defp where(_), do: ""
  defp where_fragment({k, v}) when is_list(v), do: v |> Enum.map(fn {c, v} -> where_fragment({k, c, v}) end)
  defp where_fragment({k, v}), do: ["#{k} = #{value(v)}"]
  defp where_fragment({k, :in, v}), do: "#{k} IN (#{v |> Enum.map(fn v -> value(v) end) |> Enum.join(", ")})"
  defp where_fragment({k, c, v}), do: "#{k} #{c} #{value(v)}"

  defp constrain(constraints) when is_list(constraints), do: " IF #{constraints |> Enum.map(fn {k, v} -> "#{k} = #{value(v)}" end) |> Enum.join(" AND ")}"
  defp constrain(_), do: ""

  defp if_exists(flag) when flag == true, do: " IF EXISTS"
  defp if_exists(_), do: ""

  defp value(v) when is_binary(v), do: "'#{v}'"
  defp value(v) when is_atom(v), do: ":#{v}"
  defp value(v), do: v
end
