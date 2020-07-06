defmodule Triton.Validate.Tests do
  use ExUnit.Case

  import Triton.Query
  alias __MODULE__.TestKeyspace
  alias __MODULE__.TestTable

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

    table :test_table, [keyspace: TestKeyspace] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      partition_key [:id1]
      cluster_columns [:id2]
    end
  end

  test "Coerce where in strings" do
    query =
      TestTable
      |> select(:all)
      |> where(id1: [in: ["1", "2", "3"]])

    actual_where =
      Triton.Validate.coerce(query)
      |> fn {:ok, q} -> q[:where] end.()

    assert(actual_where === [id1: [in: ["1", "2", "3"]]])
  end

  test "Coerce where in ints" do
    query =
      TestTable
      |> select(:all)
      |> where(id1: "1", id2: [in: [1, 2, 3]])

    actual_where =
      Triton.Validate.coerce(query)
      |> fn {:ok, q} -> q[:where] end.()

    assert(actual_where === [id1: "1", id2: [in: [1, 2, 3]]])
  end

  test "Coerce where in prepared" do
    query =
      TestTable
      |> prepared(p_id2s: [1, 2, 3])
      |> select(:all)
      |> where(id1: "1", id2: [in: :p_id2s])

    actual_where =
      Triton.Validate.coerce(query)
      |> fn {:ok, q} -> q[:where] end.()

    assert(actual_where === [id1: "1", id2: [in: :p_id2s]])
  end
end