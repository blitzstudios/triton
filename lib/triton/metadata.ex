defmodule Triton.Metadata do
  def metadata(schema_module) do
    Module.concat(schema_module, "Metadata")
  end

  def keyspace(schema_module) do
    meta = metadata(schema_module)
    case is_materialized_view(schema_module) do
      false ->
        meta.__struct__
          .__schema__.__struct__.__keyspace__
      true ->
        meta.__struct__
          .__from_metadata__.__struct__
          .__schema__.__struct__.__keyspace__
    end
  end

  def secondary_keyspace(schema_module) do
    meta = metadata(schema_module)
    case is_materialized_view(schema_module) do
      false ->
        meta.__struct__
          .__schema__.__struct__
          |> Map.from_struct()
          |> fn map -> map[:__dual_write_keyspace__] end.()
      true ->
        meta.__struct__
          .__from_metadata__.__struct__
          .__schema__.__struct__
          |> Map.from_struct()
          |> fn map -> map[:__dual_write_keyspace__] end.()
    end
  end

  def conn(schema_module) do
    keyspace(schema_module).__struct__.__conn__
  end

  def secondary_conn(schema_module) do
    case secondary_keyspace(schema_module) do
      nil -> nil
      keyspace -> keyspace.__struct__.__conn__
    end
  end

  def table(schema_module) do
    metadata(schema_module).__struct__.__table__
  end

  def schema(schema_module) do
    metadata(schema_module).__struct__.__schema__
  end

  def fields(schema_module) do
    fields = schema(schema_module).__struct__.__fields__
    case {is_materialized_view(schema_module), fields} do
      # MV's need to use the table format fields in order to deal
      # with transforms and coercion
      {true, :all} ->
        # A 'fields :all' MV needs to pull fields from the parent table
        metadata(schema_module).__struct__
        .__from_metadata__.__struct__
        .__schema__.__struct__
        .__fields__
      {true, fields} ->
        field_set = MapSet.new(fields)
        metadata(schema_module).__struct__
          .__from_metadata__.__struct__
          .__schema__.__struct__
          .__fields__
          |> Enum.filter(fn {k, _v} -> MapSet.member?(field_set, k) end)
          |> Enum.into(%{})
      _ -> fields
    end
  end

  def is_materialized_view(schema_module) do
    meta = metadata(schema_module)
    case meta.__struct__.__type__ do
      :table -> false
      :materialized_view -> true
    end
  end

  def transform_streams(schema_module) do
    meta = metadata(schema_module)
    case is_materialized_view(schema_module) do
      false ->
        meta.__struct__
          .__schema__.__struct__.__transform_streams__
      true ->
        meta.__struct__
          .__from_metadata__.__struct__
          .__schema__.__struct__.__transform_streams__
    end
    |> case do
      yes when yes in [true, "true"] -> true
      no when no in [false, "false"] -> false
      _ -> nil
    end
  end
end