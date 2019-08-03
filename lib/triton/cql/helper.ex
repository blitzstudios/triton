defmodule Triton.CQL.Helper do
  def field_value(nil, _), do: "NULL"
  def field_value(field, {:map, _}) when is_map(field), do: binary_value(field)

  def field_value(field, :timestamp) when is_binary(field) do
    {:ok, timestamp, _} = DateTime.from_iso8601(field)

    timestamp
    |> DateTime.to_unix(:millisecond)
    |> to_string()
  end

  def field_value(v, :counter), do: v

  def field_value(field, {_, _}), do: field
  def field_value(field, _) when is_boolean(field), do: to_string(field)
  def field_value(field, _) when is_binary(field), do: binary_value(field)
  def field_value(field, _) when is_atom(field), do: ":#{field}"
  def field_value(%DateTime{} = d, _), do: DateTime.to_unix(d, :millisecond)
  def field_value(field, _), do: to_string(field)

  def if_not_exists(flag) when flag == true, do: " IF NOT EXISTS"
  def if_not_exists(_), do: ""

  def if_exists(flag) when flag == true, do: " IF EXISTS"
  def if_exists(_), do: ""

  def binary_value(v) when is_binary(v) do
    if String.valid?(v) && String.contains?(v, ["'", "\""]) do
      "$$" <> v <> "$$"
    else
      "'" <> v <> "'"
    end
  end

  def binary_value(v) when is_map(v), do: "{" <> Enum.map_join(v, ",", &binary_value/1) <> "}"

  # This will fail if v `is_map`, nested maps are generally not OK
  def binary_value({k, v}), do: binary_value(k) <> ": " <> binary_value(v)

  def binary_value(v), do: v |> to_string() |> binary_value()
end
