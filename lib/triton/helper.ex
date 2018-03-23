defmodule Triton.Helper do
  def query_type(query) do
    cond do
      query[:stream] -> :stream
      query[:count] -> :count
      query[:select] -> :select
      query[:insert] -> :insert
      query[:update] -> :update
      query[:delete] -> :delete
      true           -> {:error, Triton.Error.invalid_cql_operation}
    end
  end
end
