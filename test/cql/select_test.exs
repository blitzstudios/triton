defmodule Triton.CQL.Select.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.CQL.Select.Tests.TestTable

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

    table :messages_by_parent, [keyspace: Triton.CQL.Select.Tests.TestKeyspace] do
      field :id1, :text
      field :id2, :bigint
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  test "Select *" do
    actual =
      TestTable
      |> select(:all)
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent")
  end

  test "Select columns" do
    actual =
      TestTable
      |> select([:id1, :id2])
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent")
  end

  test "Select where" do
    query =
      TestTable
      |> select(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = 'one' AND id2 = 2")
    assert(bindings === nil)
  end

  test "Select where prepared/2" do
    query =
      TestTable
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = :id1 AND id2 = :id2")
    assert(bindings === [id1: "one", id2: 2])
  end

  test "Select where prepared/1" do
    query =
      TestTable
      |> prepared()
      |> select(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = :w_id1_0 AND id2 = :w_id2_1")
    assert(bindings === [w_id1_0: "one", w_id2_1: 2])
  end

  test "Select where in" do
    query =
      TestTable
      |> select(:all)
      |> where(id1: [in: ["one", "two", "three"]])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 IN ('one', 'two', 'three')")
    assert(bindings === nil)
  end

  test "Select where in prepared/2" do
    query =
      TestTable
      |> prepared(id1: ["one", "two", "three"])
      |> select(:all)
      |> where(id1: [in: :id1])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 IN :id1")
    assert(bindings === [id1: ["one", "two", "three"]])
  end

  test "Select where in prepared/1" do
    query =
      TestTable
      |> prepared()
      |> select(:all)
      |> where(id1: [in: ["one", "two", "three"]])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 IN :w_id1_0")
    assert(bindings === [w_id1_0: ["one", "two", "three"]])
  end

  test "Select where range" do
    actual =
      TestTable
      |> select(:all)
      |> where(id1: "one", id2: [">=": 1], id2: ["<": 10])
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = 'one' AND id2 >= 1 AND id2 < 10")
  end

  test "Select where range prepared/2" do
    query =
      TestTable
      |> prepared(id1: "one", id2_gt: 1, id2_lt: 10)
      |> select(:all)
      |> where(id1: :id1, id2: [">=": :id2_gt], id2: ["<": :id2_lt])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = :id1 AND id2 >= :id2_gt AND id2 < :id2_lt")
    assert(bindings === [id1: "one", id2_gt: 1, id2_lt: 10])
  end

  test "Select where range prepared/1" do
    query =
      TestTable
      |> prepared()
      |> select(:all)
      |> where(id1: "one", id2: [">=": 1], id2: ["<": 10])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Select.build()
    bindings = query[:prepared]

    assert(cql === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = :w_id1_0 AND id2 >= :w_id2_1 AND id2 < :w_id2_2")
    assert(bindings === [w_id1_0: "one", w_id2_1: 1, w_id2_2: 10])
  end

  test "Select where order by" do
    actual =
      TestTable
      |> select(:all)
      |> where(id1: "one", id2: 2)
      |> order_by(id1: "desc")
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = 'one' AND id2 = 2 ORDER BY id1 desc")
  end

  test "Select limit" do
    actual =
      TestTable
      |> select(:all)
      |> limit(1)
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent LIMIT 1")
  end

  test "Select allow filtering" do
    actual =
      TestTable
      |> select(:all)
      |> allow_filtering
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent ALLOW FILTERING")
  end

  test "Select where quotes and dollars" do
    actual =
      TestTable
      |> select(:all)
      |> where(id1: "single' quotes'' should 'work' and $$dollars$$", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Select.build()

    assert(actual === "SELECT id1, id2 FROM messages_by_parent WHERE id1 = 'single'' quotes'''' should ''work'' and $$dollars$$' AND id2 = 2")
  end
end
