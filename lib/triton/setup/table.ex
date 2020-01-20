defmodule Triton.Setup.Table do
  @moduledoc """
  To test:
  Schema.User.Table.__after_compile__(nil, nil)
  """

  @doc """
  Attempts to create tables at compile time by connecting to DB with Xandra
  """
  def setup(blueprint) do
    try do
      cluster =
        Application.get_env(:triton, :clusters)
        |> Enum.find(&(&1[:conn] == blueprint.__keyspace__.__struct__.__conn__))

      setup_p(blueprint, cluster)

      if(blueprint.__dual_write_keyspace__) do
        dual_write_cluster =
          Application.get_env(:triton, :clusters)
          |> Enum.find(&(&1[:conn] == blueprint.__dual_write_keyspace__.__struct__.__conn__))

        setup_p(blueprint, dual_write_cluster)
      end

    rescue
      err -> IO.inspect(err)
    end
  end

  defp setup_p(blueprint, cluster) do
    node_config =
      cluster
      |> Keyword.take([:nodes, :authentication, :keyspace])

    node_config = Keyword.put(node_config, :nodes, [node_config[:nodes] |> Enum.random()])
    {:ok, _apps} = Application.ensure_all_started(:xandra)
    {:ok, conn} = Xandra.start_link(node_config)

    statement = build_cql(blueprint |> Map.delete(:__struct__))
    Xandra.execute!(conn, "USE #{node_config[:keyspace]};", _params = [])
    Xandra.execute!(conn, statement, _params = [])
  end

  ## PRIVATE - Build CQL

  defp build_cql(blueprint) do
    create_cql(blueprint[:__name__]) <>
    " (" <>
    fields_cql(blueprint[:__fields__]) <>
    primary_key_cql(blueprint[:__partition_key__], blueprint[:__cluster_columns__]) <>
    ")" <>
    with_options_cql(blueprint[:__with_options__])
  end

  defp create_cql(name), do: "CREATE TABLE IF NOT EXISTS #{name}"

  defp fields_cql(fields), do: fields |> Enum.map(fn field -> field_cql(field) end) |> Enum.join(", ")

  defp field_cql({field, %{type: {collection_type, type}}}), do: "#{field} #{collection_type}#{type}"
  defp field_cql({field, %{type: type}}), do: "#{field} #{type}"

  defp primary_key_cql(partition_key, cluster_columns) when is_list(partition_key) and is_list(cluster_columns) do
    ", PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "), #{Enum.join(cluster_columns, ", ")})"
  end
  defp primary_key_cql(partition_key, nil) when is_list(partition_key) do
    ", PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "))"
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
