defmodule Triton.Configuration do
  @consistency_types [
    :one,
    :two,
    :three,
    :serial,
    :all,
    :quorum,
    :local_one,
    :local_quorum,
    :each_quorum,
    :local_serial
  ]

  def disable_compilation_migrations?(), do: enabled?(:disable_compilation_migrations)
  def enable_auto_prepare?(), do: enabled?(:enable_auto_prepare)

  def enabled?(key) do
    case Application.get_env(:triton, key) do
      true -> true
      "true" -> true
      _ -> false
    end
  end

  def write_consistency(), do: consistency(:write_consistency)
  def read_consistency(), do: consistency(:read_consistency)

  def consistency(key) do
    consistency = Application.get_env(:triton, key)
    cond do
      is_atom(consistency) && valid_consistency?(consistency) ->
        consistency
      is_binary(consistency) && valid_consistency?(consistency) ->
        String.to_atom(consistency)
      true -> :one
    end
  end

  def valid_consistency?(consistency) when is_binary(consistency) do
    String.to_atom(consistency)
    |> valid_consistency?()
  end
  def valid_consistency?(consistency), do: consistency in @consistency_types
end