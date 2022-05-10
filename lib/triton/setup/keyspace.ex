defmodule Triton.Setup.Keyspace do
  def setup(schema_module) do
    blueprint = schema_module.__struct__
    try do
      cluster =
        Application.get_env(:triton, :clusters)
        |> Enum.find(&(&1[:conn] == blueprint.__conn__))
      name = cluster |> Keyword.get(:conn)
      node_config = cluster |> Keyword.put(:name, name)

      {:ok, _apps} = Application.ensure_all_started(:xandra)
      {:ok, _conn} =
        case Xandra.Cluster.start_link(node_config) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
        end

      statement = build_cql(schema_module)
      Xandra.Cluster.execute!(name, statement, _params = [])
    rescue
      err -> IO.inspect(err)
    end
  end

  def build_cql(schema_module) do
    blueprint = schema_module.__struct__ |> Map.from_struct()
    create_cql(blueprint[:__name__]) <>
    with_options_cql(blueprint[:__with_options__])
  end

  defp create_cql(name), do: "CREATE KEYSPACE IF NOT EXISTS #{name}"

  defp with_options_cql(opts) when is_list(opts) do
    cql = opts
      |> Enum.map(fn opt -> with_option_cql(opt) end)
      |> Enum.join(" AND ")

    " WITH " <> cql
  end
  defp with_options_cql(_), do: ""

  defp with_option_cql({option, value}), do: "#{String.upcase(to_string(option))} = #{value}"
end
