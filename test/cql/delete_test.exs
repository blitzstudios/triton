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
    actual =
      TestTable
      |> delete(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.CQL.Delete.build()

    assert(actual === "DELETE FROM messages_by_parent WHERE id1 = 'one' AND id2 = 2")
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
