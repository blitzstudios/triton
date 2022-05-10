defmodule Triton.Setup.MaterializedView do
  def setup(schema_module) do
    blueprint = Triton.Metadata.schema(schema_module).__struct__
    try do
      tableModule = Module.concat(blueprint.__from__, Table)
      cluster =
        Application.get_env(:triton, :clusters)
        |> Enum.find(
             &(&1[:conn] ==
                 tableModule.__struct__.__keyspace__.__struct__.__conn__)
           )

      setup_p(schema_module, cluster)

      if(dual_writes_enabled() && tableModule.__struct__.__dual_write_keyspace__) do
        dual_write_cluster =
          Application.get_env(:triton, :clusters)
          |> Enum.find(
               &(&1[:conn] ==
                   tableModule.__struct__.__dual_write_keyspace__.__struct__.__conn__)
             )

        setup_p(schema_module, dual_write_cluster)
      end

    rescue
      err -> IO.inspect(err, label: inspect(schema_module))
    end
  end

  defp setup_p(schema_module, cluster) do
    name = cluster |> Keyword.get(:conn)
    node_config =
      cluster
      |> Keyword.put(:name, name)
      |> Keyword.put(:after_connect, fn(conn) -> Xandra.execute(conn, "USE #{cluster[:keyspace]}") end)

    {:ok, _apps} = Application.ensure_all_started(:xandra)
    {:ok, _conn} =
      case Xandra.Cluster.start_link(node_config) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
      end

    statement = build_cql(schema_module)
    Xandra.Cluster.run(name, fn conn ->
      Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
      Xandra.execute!(conn, statement, _params = [])
    end)
  end

  def build_cql(schema_module) do
    blueprint = Triton.Metadata.schema(schema_module).__struct__ |> Map.from_struct
    create_cql(blueprint[:__name__]) <>
    select_cql(blueprint[:__fields__]) <>
    from_cql(blueprint[:__from__]) <>
    where_cql(blueprint[:__partition_key__], blueprint[:__cluster_columns__]) <>
    primary_key_cql(blueprint[:__partition_key__], blueprint[:__cluster_columns__]) <>
    with_options_cql(blueprint[:__with_options__])
  end

  defp create_cql(name), do: "CREATE MATERIALIZED VIEW IF NOT EXISTS #{name}"

  defp select_cql(fields) when is_list(fields), do: " AS SELECT " <> Enum.join(fields, ", ")
  defp select_cql(_), do: " AS SELECT *"

  defp from_cql(module), do: " FROM #{Module.concat(module, Table).__struct__.__name__}"

  defp where_cql(pk, cc) when is_list(pk) and is_list(cc) do
    fields_not_null = (pk ++ cc)
      |> Enum.map(fn field -> "#{field} IS NOT NULL" end)
      |> Enum.join(" AND ")

    " WHERE #{fields_not_null}"
  end
  defp where_cql(pk, _) when is_list(pk) do
    fields_not_null = pk
      |> Enum.map(fn field -> "#{field} IS NOT NULL" end)
      |> Enum.join(" AND ")

    " WHERE #{fields_not_null}"
  end
  defp where_cql(_, _), do: ""

  defp primary_key_cql(partition_key, cluster_columns) when is_list(partition_key) and is_list(cluster_columns) do
    " PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "), #{Enum.join(cluster_columns, ", ")})"
  end
  defp primary_key_cql(partition_key, nil) when is_list(partition_key) do
    " PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "))"
  end
  defp primary_key_cql(_, _), do: ""

  defp with_options_cql(opts) when is_list(opts) do
    cql = opts
      |> Enum.map(fn opt -> with_option_cql(opt) end)
      |> Enum.join(" AND ")

    " WITH " <> cql
  end
  defp with_options_cql(_), do: ""

  defp with_option_cql({:clustering_order_by, opts}) do
    fields_and_order = opts |> Enum.map(fn {field, order} -> "#{field} #{order}" end) |> Enum.join(", ")
    "CLUSTERING ORDER BY (" <> fields_and_order <> ")"
  end
  defp with_option_cql({option, value}), do: "#{String.upcase(to_string(option))} = #{value}"

  defp dual_writes_enabled() do
    case Application.get_env(:triton, :enable_dual_writes) do
      true -> true
      "true" -> true
      _ -> false
    end
  end

end
