defmodule Triton.Setup.Tests do
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
        :id2, :data
      ]
      partition_key [:id2]
    end
  end

  defmodule TestViewWhere do
    use Triton.MaterializedView

    materialized_view :test_mv_where, from: TestTable do
      fields [
        :id2, :data
      ]
      Triton.MaterializedView.where "id2 != 'test'"
      partition_key [:id2]
    end
  end

  defmodule TestViewReplicas do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :test_mv_replicas, replicas: 2, from: TestTable do
      fields [
        :id2, :data
      ]
      partition_key [:id2]
    end
  end

  test "Keyspace cql" do
    cql = Triton.Setup.Keyspace.build_cql(TestKeyspace)
    assert(cql === "CREATE KEYSPACE IF NOT EXISTS triton_tests WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3}")
  end

  test "Table cql" do
    cql = Triton.Setup.Table.build_cql(TestTable)
    assert(cql === "CREATE TABLE IF NOT EXISTS test_table (data text, id1 text, id2 bigint, PRIMARY KEY((id1), id2))")
  end

  test "MV cql" do
    cql = Triton.Setup.MaterializedView.build_cql(TestView)
    assert(cql === "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv AS SELECT id2, data FROM test_table WHERE id2 IS NOT NULL PRIMARY KEY((id2))")
  end

  test "MV where cql" do
    cql = Triton.Setup.MaterializedView.build_cql(TestViewWhere)
    assert(cql === "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv_where AS SELECT id2, data FROM test_table WHERE id2 IS NOT NULL AND id2 != 'test' PRIMARY KEY((id2))")
  end

  test "MV replicas cql" do
    cql = Triton.Setup.MaterializedView.build_cql(TestViewReplicas)
    assert(cql === "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv_replicas AS SELECT id2, data FROM test_table WHERE id2 IS NOT NULL PRIMARY KEY((id2))")
    cql = Triton.Setup.MaterializedView.build_cql(TestViewReplicas, _replicas = 1)
    assert(cql === "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv_replicas AS SELECT id2, data FROM test_table WHERE id2 IS NOT NULL PRIMARY KEY((id2))")
    cql = Triton.Setup.MaterializedView.build_cql(TestViewReplicas, _replicas = 2)
    assert(cql === "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv_replicas_2 AS SELECT id2, data FROM test_table WHERE id2 IS NOT NULL PRIMARY KEY((id2))")
  end
end