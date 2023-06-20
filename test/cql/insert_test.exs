defmodule Triton.CQL.Insert.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.CQL.Insert.Tests.TestTable

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
      field :map, {:map, "<int, text>"}
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  test "Insert" do
    query =
      TestTable
      |> insert(id1: "one", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Insert.build()
    bindings = query[:prepared]

    assert(cql === "INSERT INTO messages_by_parent (id1, id2) VALUES ('one', 2)")
    assert(bindings === nil)
  end

  test "Insert prepared/1" do
    query =
      TestTable
      |> prepared()
      |> insert(id1: "one", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Insert.build()
    bindings = query[:prepared]

    assert(cql === "INSERT INTO messages_by_parent (id1, id2) VALUES (:i_id1_0, :i_id2_1)")
    assert bindings === [i_id1_0: "one", i_id2_1: 2]
  end

  test "Insert prepared/2" do
    query =
      TestTable
      |> prepared(id1: "one", id2: 2)
      |> insert(id1: :id1, id2: :id2)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Insert.build()
    bindings = query[:prepared]

    assert(cql === "INSERT INTO messages_by_parent (id1, id2) VALUES (:id1, :id2)")
    assert bindings === [id1: "one", id2: 2]
  end

  test "Insert quotes and dollars" do
    actual =
      TestTable
      |> insert(id1: "single' quotes'' should 'work' and $$dollars$$", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Insert.build()

    assert(actual === "INSERT INTO messages_by_parent (id1, id2) VALUES ('single'' quotes'''' should ''work'' and $$dollars$$', 2)")
  end

  test "Inserts maps" do
    actual =
      TestTable
      |> insert(id1: "one", id2: 2, map: "{1: 'one'}")
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Insert.build()

    assert(actual === "INSERT INTO messages_by_parent (id1, id2, map) VALUES ('one', 2, {1: 'one'})")
  end

  test "Inserts null maps" do
    actual =
      TestTable
      |> insert(id1: "one", id2: 2, map: nil)
      |> Triton.CQL.Parameterize.parameterize!()
      |> Triton.CQL.Insert.build()

    assert(actual === "INSERT INTO messages_by_parent (id1, id2, map) VALUES ('one', 2, NULL)")
  end

  test "Inserts prepared maps" do
    query =
      TestTable
      |> prepared(id1: "one", id2: 2, map: "{1: 'one'}")
      |> insert(id1: :id1, id2: :id2, map: :map)
      |> Triton.CQL.Parameterize.parameterize!()

    cql = query |> Triton.CQL.Insert.build()
    bindings = query[:prepared]

    assert(cql === "INSERT INTO messages_by_parent (id1, id2, map) VALUES (:id1, :id2, :map)")
    assert bindings === [id1: "one", id2: 2, map: "{1: 'one'}"]
  end
end
