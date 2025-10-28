defmodule Triton.APM.Tests do
  use ExUnit.Case
  import Triton.Query
  alias Triton.APM.Tests.TestTable
  alias Triton.APM.Tests.TestSingleKeyspaceTable
  alias Triton.APM.Tests.TestView
  import ExUnit.CaptureLog

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
      partition_key [:id1]
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
      result_type: :error,
      is_batch: false,
      batch_size: 0,
      shard_number: -1,
      num_rows: 0
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
      result_type: :ok,
      is_batch: false,
      batch_size: 0,
      shard_number: -1,
      num_rows: 1
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
      result_type: :ok,
      is_batch: false,
      batch_size: 0,
      shard_number: -1,
      num_rows: 1
    }

    assert(actual_apm === expected_apm)
  end

  test "Select view" do
    actual_apm =
      TestView
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})

    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace",
      dml_type: "select",
      schema: "test_view",
      result_type: :ok,
      is_batch: false,
      batch_size: 0,
      shard_number: 1,
      num_rows: 0
    }

    assert(actual_apm === expected_apm)
  end

  test "Insert batch" do
    queries =
      1..10
      |> Enum.map(fn i ->
           TestSingleKeyspaceTable
           |> insert(id1: to_string(i), id2: i)
         end)

    actual_apm =
      Triton.APM.from_query!(
        Enum.at(queries, 0),
        TritonTests.Conn,
        1000,
        {:ok, :success},
        Enum.count(queries))

    expected_apm = %Triton.APM{
      duration_ms: 1000,
      keyspace: "Elixir.Triton.APM.Tests.TestKeyspace",
      dml_type: "insert",
      schema: "test_single_keyspace_table",
      result_type: :ok,
      is_batch: true,
      batch_size: 10,
      shard_number: -1,
      num_rows: 1
     }

    assert(actual_apm === expected_apm)
  end

  test "logging shard_info works" do
    logs = capture_log(fn ->
      Application.put_env(:triton, :debug_shards, [shards: :all, tables: :all])

      TestSingleKeyspaceTable
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})
    end)

    assert logs =~ "Triton shard info:"
    assert logs =~ "table: :test_single_keyspace_table"
    assert logs =~ "shard_number: 24"
    assert logs =~ "partition_key: [text: \"one\"]"
  end

  test "logging shard_info captures only configured shards and tables" do
    logs = capture_log(fn ->
      Application.put_env(:triton, :debug_shards, [shards: [24], tables: [:test_single_keyspace_table]])

      TestSingleKeyspaceTable
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})
    end)

    assert logs =~ "Triton shard info:"
    assert logs =~ "table: :test_single_keyspace_table"
    assert logs =~ "shard_number: 24"
    assert logs =~ "partition_key: [text: \"one\"]"

    logs = capture_log(fn ->
      Application.put_env(:triton, :debug_shards, [shards: [1], tables: [:test_single_keyspace_table]])

      TestSingleKeyspaceTable
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})
    end)
    assert logs == ""

    logs = capture_log(fn ->
      Application.put_env(:triton, :debug_shards, [shards: [1], tables: [:test_single_keyspace_table]])

      TestSingleKeyspaceTable
      |> prepared(id1: "one", id2: 2)
      |> select(:all)
      |> where(id1: :id1, id2: :id2)
      |> Triton.APM.from_query!(TritonTests.Conn, 1000, {:ok, []})
    end)
    assert logs == ""
  end
end
