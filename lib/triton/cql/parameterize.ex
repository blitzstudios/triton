defmodule Triton.CQL.Parameterize do
  def parameterize!(query) do
    if query[:prepared] == :auto do
      [:insert, :update, :where]
      |> Enum.reduce(Keyword.put(query, :prepared, []), fn statement_type, acc ->
           case query[statement_type] do
             nil -> acc
             _statement ->
               {parameterized, bindings} = parameterize_for_type(query, statement_type)
               merged_bindings = acc[:prepared] ++ bindings

               acc
               |> Keyword.put(statement_type, parameterized)
               |> Keyword.put(:prepared, merged_bindings)
           end
         end)
    else
      query
    end
  end

  defp parameterize_for_type(query, statement_type) do
    tokenized =
      query[statement_type]
      |> Enum.with_index()
      |> Enum.map(fn {{k, v}, i} -> {k, v, tokenize(k, statement_type, i)} end)

    parameterized =
      tokenized
      |> Enum.map(fn
           {k, [{operator, _v}], token} -> {k, [{operator, token}]}
           {k, _v, token} -> {k, token}
         end)

    bindings =
      tokenized
      |> Enum.map(fn
           {_k, [{_operator, v}], token} -> {token, v}
           {_k, v, token} -> {token, v}
         end)

    {parameterized, bindings}
  end

  defp tokenize(key, statement_type, suffix) do
    case statement_type do
      :where -> String.to_atom("w_#{key}_#{suffix}")
      :update -> String.to_atom("u_#{key}_#{suffix}")
      :insert -> String.to_atom("i_#{key}_#{suffix}")
    end
  end
end
