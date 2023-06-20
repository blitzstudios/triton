defmodule Triton.CQL.Delete.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.CQL.Delete.Tests.TestTable

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
      field :data, :text
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  test "Delete" do
    query =
      TestTable
      |> delete(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 = 'one' AND id2 = 2")
    assert(bindings === nil)
  end

  test "Delete where in" do
    query =
      TestTable
      |> delete(:all)
      |> where(id1: [in: ["one", "two", "three"]])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 IN ('one', 'two', 'three')")
    assert(bindings === nil)
  end

  test "Delete where in prepared/2" do
    query =
      TestTable
      |> prepared(id1: ["one", "two", "three"])
      |> delete(:all)
      |> where(id1: [in: :id1])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 IN :id1")
    assert(bindings === [id1: ["one", "two", "three"]])
  end

  test "Delete where in prepared/1" do
    query =
      TestTable
      |> prepared()
      |> delete(:all)
      |> where(id1: [in: ["one", "two", "three"]])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 IN :w_id1_0")
    assert(bindings === [w_id1_0: ["one", "two", "three"]])
  end


  test "Delete where range" do
    actual =
      TestTable
      |> delete(:all)
      |> where(id1: "one", id2: [">=": 1], id2: ["<": 10])
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Delete.build()

    assert(actual === "DELETE FROM messages_by_parent WHERE id1 = 'one' AND id2 >= 1 AND id2 < 10")
  end

  test "Delete where range prepared/2" do
    query =
      TestTable
      |> prepared(id1: "one", id2_gt: 1, id2_lt: 10)
      |> delete(:all)
      |> where(id1: :id1, id2: [">=": :id2_gt], id2: ["<": :id2_lt])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 = :id1 AND id2 >= :id2_gt AND id2 < :id2_lt")
    assert(bindings === [id1: "one", id2_gt: 1, id2_lt: 10])
  end

  test "Delete where range prepared/1" do
    query =
      TestTable
      |> prepared()
      |> delete(:all)
      |> where(id1: "one", id2: [">=": 1], id2: ["<": 10])
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Delete.build()
    bindings = query[:prepared]

    assert(cql === "DELETE FROM messages_by_parent WHERE id1 = :w_id1_0 AND id2 >= :w_id2_1 AND id2 < :w_id2_2")
    assert(bindings === [w_id1_0: "one", w_id2_1: 1, w_id2_2: 10])
  end

  test "Update quotes and dollars" do
    actual =
      TestTable
      |> delete(:all)
      |> where(id1: "single' quotes'' should 'work' and $$dollars$$", id2: 2)
      |> Triton.CQL.Delete.build()

    assert(actual === "DELETE FROM messages_by_parent WHERE id1 = 'single'' quotes'''' should ''work'' and $$dollars$$' AND id2 = 2")
  end

end
