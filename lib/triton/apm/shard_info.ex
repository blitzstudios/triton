defmodule Triton.APM.ShardInfo do
  @moduledoc """
  Native functions for computing partition tokens using Rust NIFs.
  """

  use Rustler, otp_app: :triton, crate: :triton_apm_shardinfo

  # When your NIF is loaded, it will override this function.
  def shard_from_partition_key(_data), do: :erlang.nif_error(:nif_not_loaded)

  def shard_number(query) do
    case {Keyword.get(query, :where), Keyword.get(query, :prepared)} do
      {where, prepared} when not is_nil(where) and not is_nil(prepared) ->
        schema_module = query[:__schema_module__]
        partition_key = (Triton.Metadata.schema(schema_module) |> Map.from_struct())[:__partition_key__]
        IO.inspect(partition_key, label: "partition_key_in_from_query!")
        IO.inspect(query, label: "query_in_from_query!")
        partition_key
          |> Enum.map(fn(key) ->
            key_in_prepared = where[key]
            prepared[key_in_prepared]
        end)
        |> Enum.map(fn(value) -> serialize_component(value) end)
        |> shard_from_partition_key()
      _ ->
        -1
    end
  end

  defp serialize_component(value) when is_integer(value), do: <<value::integer-size(64)>>
  defp serialize_component(value) when is_binary(value), do: value
end
