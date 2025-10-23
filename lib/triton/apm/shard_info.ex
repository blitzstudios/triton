defmodule Triton.APM.ShardInfo do
  @moduledoc """
  Native functions for computing partition tokens using Rust NIFs.
  """

  use Rustler, otp_app: :triton, crate: :triton_apm_shardinfo
  require Logger

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
    schema_module = query[:__schema_module__]

    schema_metadata = Triton.Metadata.schema(schema_module)
    |> Map.from_struct()

    fields = Triton.Metadata.fields(schema_module)

    Map.get(schema_metadata, :__partition_key__)
    |> Enum.map(fn key ->
      {key, fields[key][:type]}
    end)
  end

  @spec partition_key_values(list(), keyword(), keyword()) :: {:ok, list()} | {:error, :cannot_extract_partition_key_values}
  defp partition_key_values(partition_key, where, prepared) do
    get_value = fn({key, type}, where, prepared) ->
      # with proper parametrization should be in the 'prepared', but possible to have just in the 'where' block
      # I fixed a couple that discovered when running tests, but placing a guard here in case there are more cases
      # not caught by the tests.
      transitive_get = fn
        key when is_atom(key) -> prepared[key]
        key -> key
      end

      with where_key <- where[key],
           true <- not is_nil(where_key) and not is_list(where_key),
           value <- transitive_get.(where_key),
           false <- is_nil(value)
      do
        serialize_component(type, value)
      else
        _ -> nil
      end
    end

    values = partition_key
      |> Enum.map(&get_value.(&1, where, prepared))
      |> Enum.filter(& &1)

    if length(values) == length(partition_key) do
      {:ok, values}
    else
      {:error, :cannot_extract_partition_key_values}
    end
  end

  def serialize_component(type, value) do
    Xandra.Protocol.V4.encode_query_value(type, value) |> List.last()
  end
end
