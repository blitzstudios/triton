defmodule Triton.TransformStream.Tests do
  use ExUnit.Case, async: false
  import Triton.Query
  alias __MODULE__.TestKeyspace
  alias __MODULE__.TestTable

  def to_string(s), do: Kernel.to_string(s)
  defp execute_cql(cql) do
    {:ok, _apps} = Application.ensure_all_started(:xandra)
    {:ok, conn} =
      Application.get_env(:triton, :clusters)
      |> Enum.find(fn cluster -> cluster[:conn] == TritonTests.Conn end)
      |> Keyword.take([:nodes])
      |> Xandra.start_link()

    Xandra.execute(conn, cql)
  end

  defp drop_test_keyspace(), do: execute_cql("drop keyspace if exists triton_tests;")
  defp truncate_test_tables() do
    execute_cql("truncate triton_tests.stream_transform_test_table")
    execute_cql("truncate triton_tests.stream_transform_test_table_with_transform_true")
    execute_cql("truncate triton_tests.stream_transform_test_table_with_transform_false")
  end

  defmodule TestKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: TritonTests.Conn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule TestTable do
    use Triton.Table
    import Triton.Query

    table :stream_transform_test_table, [keyspace: TestKeyspace] do
      field :id, :text
      field :transformed, :int, transform: &Triton.TransformStream.Tests.to_string/1
      partition_key [:id]
    end
  end

  defmodule TestTableWithTransformStreams do
    use Triton.Table
    import Triton.Query

    table :stream_transform_test_table_with_transform_true, [keyspace: TestKeyspace, transform_streams: true] do
      field :id, :text
      field :transformed, :int, transform: &Triton.TransformStream.Tests.to_string/1
      partition_key [:id]
    end
  end

  defmodule TestTableWithTransformStreamsFalse do
    use Triton.Table
    import Triton.Query

    table :stream_transform_test_table_with_transform_false, [keyspace: TestKeyspace, transform_streams: false] do
      field :id, :text
      field :transformed, :int, transform: &Triton.TransformStream.Tests.to_string/1
      partition_key [:id]
    end
  end

  defmodule TestView do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :stream_transform_test_mv, from: TestTable do
      fields [
        :id, :transformed
      ]
      partition_key [:transformed]
      cluster_columns [:id]
    end
  end

  defmodule TestViewWithTransformStreams do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :stream_transform_test_mv_with_transform_True, from: TestTableWithTransformStreams do
      fields [
        :id, :transformed
      ]
      partition_key [:transformed]
      cluster_columns [:id]
    end
  end

  setup do
#    drop_test_keyspace()
    Triton.Setup.Keyspace.setup(TestKeyspace)
    Triton.Setup.Table.setup(TestTable)
    Triton.Setup.Table.setup(TestTableWithTransformStreams)
    Triton.Setup.Table.setup(TestTableWithTransformStreamsFalse)
    Triton.Setup.MaterializedView.setup(TestView)
    Triton.Setup.MaterializedView.setup(TestViewWithTransformStreams)
    {:ok, _} = truncate_test_tables()

    :ok
  end

  test "transform_streams without transform setting true should not transform" do
    {:ok, _} = execute_cql("insert into triton_tests.stream_transform_test_table(id, transformed) values ('1', 2)")

    expected = %{id: "1", transformed: 2}
    actual =
      TestTable
      |> select(:all)
      |> where(id: "1")
      |> TestTable.stream(page_size: 1)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected

    # MV
    actual =
      TestView
      |> select(:all)
      |> where(transformed: 2)
      |> TestView.stream(page_size: 1)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected
  end

  test "transform_streams true should transform" do
    {:ok, _} = execute_cql("insert into triton_tests.stream_transform_test_table_with_transform_true(id, transformed) values ('1', 2)")
    {:ok, _} = execute_cql("insert into triton_tests.stream_transform_test_table_with_transform_false(id, transformed) values ('1', 2)")

    expected = %{id: "1", transformed: "2"}
    actual =
      TestTableWithTransformStreams
      |> select(:all)
      |> where(id: "1")
      |> TestTableWithTransformStreams.stream(page_size: 1)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected

    # MV
    actual =
      TestViewWithTransformStreams
      |> select(:all)
      |> where(transformed: "2")
      |> TestViewWithTransformStreams.stream(page_size: 1)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected

    # Table with explicit false
    actual =
      TestTableWithTransformStreamsFalse
      |> select(:all)
      |> where(id: "1")
      |> TestTableWithTransformStreamsFalse.stream(page_size: 1, transform_streams: true)
      |> elem(1)
      |> Enum.at(0)
    assert actual == expected
  end

  test "on query, transform_streams true, should transform" do
    {:ok, _} = execute_cql("insert into triton_tests.stream_transform_test_table(id, transformed) values ('1', 2)")
    {:ok, _} = execute_cql("insert into triton_tests.stream_transform_test_table_with_transform_false(id, transformed) values ('1', 2)")

    assert Triton.Metadata.transform_streams(TestTable) === nil
    assert Triton.Metadata.transform_streams(TestTableWithTransformStreamsFalse) === false

    expected = %{id: "1", transformed: "2"}
    actual =
      TestTable
      |> select(:all)
      |> where(id: "1")
      |> TestTable.stream(page_size: 1, transform_streams: true)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected

    expected = %{id: "1", transformed: "2"}
    actual =
      TestTableWithTransformStreamsFalse
      |> select(:all)
      |> where(id: "1")
      |> TestTableWithTransformStreamsFalse.stream(page_size: 1, transform_streams: true)
      |> elem(1)
      |> Enum.at(0)

    assert actual == expected
  end
end