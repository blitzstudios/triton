defmodule Triton.CQL.Encode do
  def encode(nil), do: "NULL"
  def encode(v) when is_boolean(v), do: "#{v}"
  def encode(v) when is_binary(v), do: binary_value(v)
  def encode(v) when is_atom(v), do: ":#{v}"
  def encode(%DateTime{} = d), do: DateTime.to_unix(d, :millisecond)
  def encode(v), do: v

  defp binary_value(v) do
    cond do
      String.valid?(v) && String.contains?(v, ["'"]) -> "'#{String.replace(v, "'", "''")}'"
      true -> "'#{v}'"
    end
  end
end
