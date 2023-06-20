defmodule Triton.Metadata.Test do
  use ExUnit.Case

  defmodule TestKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: TritonTests.PrimaryConn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule TestSecondaryKeyspace do
    use Triton.Keyspace

    keyspace :triton_secondary_tests, conn: TritonTests.SecondaryConn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule TestTable do
    use Triton.Table

    table :test_table, [keyspace: TestKeyspace, dual_write_keyspace: TestSecondaryKeyspace] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      partition_key [:id1]
      cluster_columns [:id2]
    end
  end

  defmodule TestView do
    use Triton.MaterializedView

    materialized_view :test_mv, from: TestTable do
      fields [
        :id2
      ]
      partition_key [:id2]
    end
  end

  defmodule TestTableWithTransformStreams do
    use Triton.Table

    table :test_table, [transform_streams: true] do
      field :id1, :text
      partition_key [:id1]
    end
  end

  defmodule TestTableWithTransformStreamsFalse do
    use Triton.Table

    table :test_table, [transform_streams: false] do
      field :id1, :text
      partition_key [:id1]
    end
  end

  test "should get metadata" do
    assert(Triton.Metadata.metadata(TestTable) === TestTable.Metadata)
    assert(Triton.Metadata.metadata(TestView) === TestView.Metadata)
  end

  test "should get keyspace" do
    assert(Triton.Metadata.keyspace(TestTable) === TestKeyspace)
    assert(Triton.Metadata.keyspace(TestView) === TestKeyspace)
  end

  test "should get secondary keyspace" do
    assert(Triton.Metadata.secondary_keyspace(TestTable) === TestSecondaryKeyspace)
    assert(Triton.Metadata.secondary_keyspace(TestView) === TestSecondaryKeyspace)
  end

  test "should get primary conn" do
    assert(Triton.Metadata.conn(TestTable) === TritonTests.PrimaryConn)
    assert(Triton.Metadata.conn(TestView) === TritonTests.PrimaryConn)
  end

  test "should get secondary conn" do
    assert(Triton.Metadata.secondary_conn(TestTable) === TritonTests.SecondaryConn)
    assert(Triton.Metadata.secondary_conn(TestView) === TritonTests.SecondaryConn)
  end

  test "should get schema" do
    assert(Triton.Metadata.schema(TestTable) === TestTable.Table)
    assert(Triton.Metadata.schema(TestView) === TestView.MaterializedView)
  end

  test "should get fields" do
    table_fields = %{
      data: %{opts: [], type: :text},
      id1: %{opts: [], type: :text},
      id2: %{opts: [], type: :bigint}
    }
    view_fields = %{
      id2: %{opts: [], type: :bigint}
    }
    assert(Triton.Metadata.fields(TestTable) === table_fields)
    assert(Triton.Metadata.fields(TestView) === view_fields)
  end

  test "verify transform_streams table setting" do
    assert(Triton.Metadata.transform_streams(TestTable) === nil)
    assert(Triton.Metadata.transform_streams(TestTableWithTransformStreams) === true)
    assert(Triton.Metadata.transform_streams(TestTableWithTransformStreamsFalse) === false)
  end
end