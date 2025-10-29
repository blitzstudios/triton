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
          {:ok, pk_values} <- partition_key_values(partition_key, where, prepared),
          serialized_pk_values <- Enum.map(pk_values, fn {type, value} -> serialize_component(type, value) end),
          shard_number when is_integer(shard_number) <- shard_from_partition_key(serialized_pk_values) # nif can also return an error
    do
      maybe_log(query, shard_number, pk_values)
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
        {type, value}
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


  defp maybe_log(query, shard_number, partition_key) do
    with debug_config when not is_nil(debug_config) <- Application.get_env(:triton, :debug_shards),
         {:ok, shards} <- get_shards(debug_config),
         {:ok, tables} <- get_tables(debug_config),
         true <- should_log?(shards, tables, query, shard_number) do
      # want to record table, query, partition key and shard number
      log_data = %{
        table: query[:__table__],
        partition_key: partition_key,
        shard_number: shard_number
      }
      Logger.info("Triton shard info: #{inspect(log_data)}")
    else
      _ -> :noop
    end
  end

  defp get_shards(debug_config) do
    case debug_config[:shards] do
      :all -> {:ok, :all}
      shards when is_list(shards) -> {:ok, shards}
      _ -> {:error, :invalid_shards}
    end
  end

  defp get_tables(debug_config) do
    case debug_config[:tables] do
      :all -> {:ok, :all}
      tables when is_list(tables) -> {:ok, tables}
      _ -> {:error, :invalid_tables}
    end
  end

  defp should_log?(shards, tables, query, shard_number) do
    (shards == :all or Enum.member?(shards, shard_number)) and
    (tables == :all or Enum.member?(tables, query[:__table__]))
  end

  def serialize_component(:boolean, value) do
    if value == true do
      <<1>>
    else
      <<0>>
    end
  end

  def serialize_component(type, value) do
    Xandra.Protocol.V4.encode_query_value(type, value) |> List.last()
  end
end
