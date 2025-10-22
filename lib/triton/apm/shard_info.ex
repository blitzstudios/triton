defmodule Triton.APM.ShardInfo do
  @moduledoc """
  Native functions for computing partition tokens using Rust NIFs.
  """

  use Rustler, otp_app: :triton, crate: :triton_apm_shardinfo

  # When your NIF is loaded, it will override this function.
  def shard_from_partition_key(_data), do: :erlang.nif_error(:nif_not_loaded)
  def test_partition_token(_data), do: :erlang.nif_error(:nif_not_loaded)

  @spec shard_number(keyword()) :: integer()
  def shard_number(query) do
    with  where <- Keyword.get(query, :where),
          prepared <- Keyword.get(query, :prepared),
          true <- not is_nil(where) and not is_nil(prepared),
          partition_key <- partition_key(query),
          {:ok, partition_key_values} <- partition_key_values(partition_key, where, prepared),
          shard_number when is_integer(shard_number) <- shard_from_partition_key(partition_key_values) # nif can also return an error
      do
        shard_number
      else
        _ -> -1
      end
  end

  defp partition_key(query) do
    Triton.Metadata.schema(query[:__schema_module__])
    |> Map.from_struct()
    |> Map.get(:__partition_key__)
  end

  @spec partition_key_values(list(), keyword(), keyword()) :: {:ok, list()} | {:error, :cannot_extract_partition_key_values}
  defp partition_key_values(partition_key, where, prepared) do
    values =
       partition_key
      |> Enum.map(fn(key) ->
        where_key = where[key]
        if is_nil(where_key) || is_list(where_key) do
          nil
        else
          prepared[where_key]
        end
      end)
      |> Enum.filter(& &1)
    if length(values) == length(partition_key) do
      {:ok, values |> Enum.map(&serialize_component(&1))}
    else
      {:error, :cannot_extract_partition_key_values}
    end
  end

  def serialize_component(value) when is_integer(value), do: <<value::integer-size(64)>>
  def serialize_component(value) when is_binary(value), do: value
end
