defmodule Triton.Setup.MaterializedView do
  def setup(blueprint) do
    try do
      node_config =
        Application.get_env(:triton, :clusters)
        |> Enum.find(
          &(&1[:conn] ==
              Module.concat(blueprint.__from__, Table).__struct__.__keyspace__.__struct__.__conn__)
        )
        |> Keyword.take([:nodes, :authentication, :keyspace])

      node_config = Keyword.put(node_config, :nodes, [node_config[:nodes] |> Enum.random()])
      {:ok, conn} = Xandra.start_link(node_config)

      statement = build_cql(blueprint |> Map.delete(:__struct__))
      Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
      Xandra.execute!(conn, statement, _params = [])
    rescue
      err -> IO.inspect(err)
    end
  end

  defp build_cql(blueprint) do
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
end
