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

  defmodule TestView do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :test_mv, from: TestTable do
      fields [
        :id1, :id2, :data
      ]
      partition_key [:id2]
      cluster_columns [:id1]
    end
  end

  defmodule TestViewAllFields do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :test_mv, from: TestTable do
      fields :all
      partition_key [:id2]
      cluster_columns [:id1]
    end
  end

  test "Coerce where in strings" do
    query =
      TestTable
      |> select(:all)
      |> where(id1: [in: ["1", "2", "3"]])
      |> Triton.CQL.Parameterize.parameterize!()

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
      |> Triton.CQL.Parameterize.parameterize!()

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
      |> Triton.CQL.Parameterize.parameterize!()

    actual_where =
      Triton.Validate.coerce(query)
      |> fn {:ok, q} -> q[:where] end.()

    assert(actual_where === [id1: "1", id2: [in: :p_id2s]])
  end

  test "Should coerce noop mv successfully" do
    query =
      TestView
      |> select(:all)
      |> where(id1: "1", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    result = Triton.Validate.coerce(query)

    assert(result === {:ok, query})
  end

  test "Should coerce noop all fields mv successfully" do
    query =
      TestViewAllFields
      |> select(:all)
      |> where(id1: "1", id2: 2)
      |> Triton.CQL.Parameterize.parameterize!()

    result = Triton.Validate.coerce(query)

    assert(result === {:ok, query})
  end
end