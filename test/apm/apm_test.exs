defmodule Triton.APM.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.APM.Tests.TestTable
  alias Triton.APM.Tests.TestSingleKeyspaceTable
  alias Triton.APM.Tests.TestView

  defmodule TestKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: TritonTests.Conn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule TestKeyspace2 do
    use Triton.Keyspace

    keyspace :triton_tests2, conn: TritonTests.Conn2 do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule TestTable do
    use Triton.Table

    table :test_table, [keyspace: Triton.APM.Tests.TestKeyspace, dual_write_keyspace: Triton.APM.Tests.TestKeyspace2] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  defmodule TestSingleKeyspaceTable do
    use Triton.Table

    table :test_single_keyspace_table, [keyspace: Triton.APM.Tests.TestKeyspace] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      partition_key [:id]
      cluster_columns [:id2]
    end
  end

  defmodule TestView do
    use Triton.MaterializedView

    materialized_view :test_view, from: TestTable do
      fields :all
      partition_key [:id2]
      cluster_columns [:id1]
    end
  end

  test "Delete table with primary keyspace" do
    actual_apm =
      TestTable
      |> delete(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:error, "something broke"})

    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace",
      dml_type: "delete",
      schema: "test_table",
      result_type: :error
    }

    assert(actual_apm === expected_apm)
  end

  test "Delete table with secondary keyspace" do
    actual_apm =
      TestTable
      |> delete(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.APM.from_query!(TritonTests.Conn2, 1000, {:ok, :success})

    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace2",
      dml_type: "delete",
      schema: "test_table",
      result_type: :ok
    }

    assert(actual_apm === expected_apm)
  end

  test "Delete table with single keyspace" do
    actual_apm =
      TestSingleKeyspaceTable
      |> delete(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, :success})


    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace",
      dml_type: "delete",
      schema: "test_single_keyspace_table",
      result_type: :ok
    }

    assert(actual_apm === expected_apm)
  end

  test "Select view" do
    actual_apm =
     TestView
      |> select(:all)
      |> where(id1: "one", id2: 2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})

    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace",
      dml_type: "select",
      schema: "test_view",
      result_type: :ok
     }

    assert(actual_apm === expected_apm)
  end

end
