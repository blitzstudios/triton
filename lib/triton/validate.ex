defmodule Triton.Validate do
  def coerce(query) do
    with {:ok, query} <- validate(query) do
      fields = query[:__schema__].__fields__
      {:ok, Enum.map(query, fn x -> coerce(x, fields) end)}
    end
  end

  def validate(query) do
    case Triton.Helper.query_type(query) do
      {:error, err} -> {:error, err.message}
      type ->
        case Application.get_env(:triton, :disable_validation) do
          true -> {:ok, query}
          _ -> validate(type, query, query[:__schema__].__fields__)
        end
    end
  end

  def validate(:insert, query, schema) do
    data = query[:prepared] && query[:prepared] ++ (query[:insert] |> Enum.filter(fn {_, v} -> !is_atom(v) end)) || query[:insert]
    vex = schema |> Enum.filter(fn({_, opts}) -> opts[:opts][:validators] end) |> Enum.map(fn {field, opts} -> {field, opts[:opts][:validators]} end)
    case Vex.errors(data ++ [_vex: vex]) do
      [] -> {:ok, query}
      err_list -> {:error, err_list |> Triton.Error.vex_error}
    end
  end
  def validate(:update, query, schema) do
    data = query[:prepared] && query[:prepared] ++ (query[:update] |> Enum.filter(fn {_, v} -> !is_atom(v) end)) || query[:update]
    fields_to_validate = data |> Enum.map(&(elem(&1, 0)))
    vex = schema |> Enum.filter(fn({_, opts}) -> opts[:opts][:validators] end) |> Enum.map(fn {field, opts} -> {field, opts[:opts][:validators]} end) |> Enum.filter(&(elem(&1, 0) in fields_to_validate))
    case Vex.errors(data ++ [_vex: vex]) do
      [] -> {:ok, query}
      err_list -> {:error, err_list |> Triton.Error.vex_error}
    end
  end
  def validate(_, query, _), do: {:ok, query}

  defp coerce({:__schema__, v}, _), do: {:__schema__, v}
  defp coerce({:__table__, v}, _), do: {:__table__, v}
  defp coerce({k, v}, fields), do: {k, coerce(v, fields)}

  defp coerce(fragments, fields) when is_list(fragments), do: fragments |> Enum.map(fn fragment -> coerce_fragment(fragment, fields) end)
  defp coerce(non_list, _), do: non_list

  defp coerce_fragment({k, vs}, fields) when is_list(vs) do
    coerced =
      vs
      |> Enum.map(fn
           {c, v} -> coerce_fragment({k, c, v}, fields)
           # This happens when a prepared where in binding is coerced
           #    TestTable
           #    |> prepared(p_id2s: [1, 2, 3])
           #    |> select(:all)
           #    |> where(id1: "1", id2: [in: :p_id2s])
           v -> coerced_value(v, fields[k][:type])
      end)

    {k, coerced}
  end
  defp coerce_fragment({k, v}, fields), do: {k, coerced_value(v, fields[k][:type])}
  defp coerce_fragment({k, c, vs}, fields) when is_list(vs), do: {c, vs |> Enum.map(fn v -> coerced_value(v, fields[k][:type]) end)}
  defp coerce_fragment({k, c, v}, fields), do: {c, coerced_value(v, fields[k][:type])}
  defp coerce_fragment(x, _), do: x

  defp coerced_value(value, _) when is_atom(value), do: value
  defp coerced_value(value, :text) when not is_binary(value), do: to_string(value)
  defp coerced_value(value, :bigint) when is_binary(value), do: String.to_integer(value)
  defp coerced_value(value, :int) when is_binary(value), do: String.to_integer(value)
  defp coerced_value(value, :smallint) when is_binary(value), do: String.to_integer(value)
  defp coerced_value(value, :varint) when is_binary(value), do: String.to_integer(value)
  defp coerced_value(value, _), do: value
end
