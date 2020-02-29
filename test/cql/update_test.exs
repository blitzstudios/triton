defmodule Triton.CQL.Update.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.CQL.Update.Tests.TestTable

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
      field :map, {:map, "<int, text>"}
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  test "Update" do
    actual =
      TestTable
      |> update(data: "payload")
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Update.build()

    assert(actual === "UPDATE messages_by_parent SET data = 'payload' WHERE id1 = 'one' AND id2 = 2")
  end

  test "Update quotes and dollars" do
    actual =
      TestTable
      |> update(data: "payload with single' quotes'' should 'work' and $$dollars$$")
      |> where(id1: "single' quotes'' should 'work' and $$dollars$$", id2: 2)
      |> Triton.CQL.Update.build()

    assert(actual === "UPDATE messages_by_parent SET data = 'payload with single'' quotes'''' should ''work'' and $$dollars$$' WHERE id1 = 'single'' quotes'''' should ''work'' and $$dollars$$' AND id2 = 2")
  end

  test "Updates maps" do
    actual =
      TestTable
      |> update(map: "{1: 'one'}")
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Update.build()

    assert(actual === "UPDATE messages_by_parent SET map = {1: 'one'} WHERE id1 = 'one' AND id2 = 2")
  end

  test "Updates null maps" do
    actual =
      TestTable
      |> update(map: nil)
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Update.build()

    assert(actual === "UPDATE messages_by_parent SET map = NULL WHERE id1 = 'one' AND id2 = 2")
  end

end
