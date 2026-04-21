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
      err -> IO.inspect(err)
    end
  end

  defp setup_p(schema_module, cluster) do
    node_config =
      cluster
      |> Keyword.take([:nodes, :authentication, :keyspace])

    node_config = Keyword.put(node_config, :nodes, [node_config[:nodes] |> Enum.random()])
    {:ok, _apps} = Application.ensure_all_started(:xandra)
    {:ok, conn} = Xandra.start_link(node_config)

    replicas = Triton.Metadata.replicas(schema_module)
    (1..replicas)
    |> Enum.map(fn replica ->
      statement = build_cql(schema_module, replica)
      Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
      Xandra.execute!(conn, statement, _params = [])
    end)
  end

  def build_cql(schema_module, replica_number \\ 1) do
    blueprint = Triton.Metadata.schema(schema_module).__struct__ |> Map.from_struct
    create_cql(blueprint[:__name__], replica_number) <>
    select_cql(blueprint[:__fields__]) <>
    from_cql(blueprint[:__from__]) <>
    where_cql(blueprint[:__partition_key__], blueprint[:__cluster_columns__], blueprint[:__where__]) <>
    primary_key_cql(blueprint[:__partition_key__], blueprint[:__cluster_columns__]) <>
    with_options_cql(blueprint[:__with_options__])
  end

  defp create_cql(name, _replica = 1), do: "CREATE MATERIALIZED VIEW IF NOT EXISTS #{name}"
  defp create_cql(name, replica), do: "CREATE MATERIALIZED VIEW IF NOT EXISTS #{name}_#{replica}"

  defp select_cql(fields) when is_list(fields), do: " AS SELECT " <> Enum.join(fields, ", ")
  defp select_cql(_), do: " AS SELECT *"

  defp from_cql(module), do: " FROM #{Module.concat(module, Table).__struct__.__name__}"

  defp where_cql(pk, cc, additional_restrictions) do
    pk = pk || []
    cc = cc || []
    additional_restrictions = additional_restrictions || ""

    fields_not_null = (pk ++ cc)
      |> Enum.map(fn field -> "#{field} IS NOT NULL" end)
      |> Enum.join(" AND ")

    predicate =
     [fields_not_null, additional_restrictions]
     |> Enum.filter(fn s -> s not in [nil, ""] end)
     |> Enum.join(" AND ")

    case predicate do
      "" -> ""
      _ -> " WHERE #{predicate}"
    end
  end

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
