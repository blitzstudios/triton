defmodule Triton.Executor.Tests do
  use ExUnit.Case, async: false
  @moduletag :integration

  import Triton.Query
  alias __MODULE__.TestKeyspace
  alias __MODULE__.TestTable
  alias __MODULE__.TestView

  defmodule TestKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: TritonTests.Conn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  # Couldn't reference the macro directly
  def to_string(s), do: Kernel.to_string(s)

  defmodule TestTable do
    use Triton.Table
    import Triton.Query

    table :test_table, [keyspace: TestKeyspace] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      field :map, {:map, "<int, text>"}
      field :transformed, :int, transform: &Triton.Executor.Tests.to_string/1
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

  defp execute_cql(cql) do
    {:ok, _apps} = Application.ensure_all_started(:xandra)
    {:ok, conn} =
      Application.get_env(:triton, :clusters)
      |> Enum.find(fn cluster -> cluster[:conn] == TritonTests.Conn end)
      |> Keyword.take([:nodes])
      |> Xandra.start_link()

    Xandra.execute(conn, cql)
  end

  defp drop_test_keyspace(), do: execute_cql("drop keyspace if exists triton_tests")
  defp truncate_test_table(), do: execute_cql("truncate triton_tests.test_table")

  setup do
#    Application.put_env(:triton, :enable_auto_prepare, true)
    #drop_test_keyspace()
    Triton.Setup.Keyspace.setup(TestKeyspace)
    Triton.Setup.Table.setup(TestTable)
    Triton.Setup.MaterializedView.setup(TestView)
    {:ok, _} = truncate_test_table()
    :ok
  end

  test "Select" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 2, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('4', 5, 'six')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('7', 8, 'nine')")

    expected = %{id1: "1", id2: 2, data: "three", map: nil, transformed: ""}

    actual =
      TestTable
      |> select(:all)
      |> where(id1: "1", id2: 2)
      |> TestTable.one

    assert(actual === {:ok, expected})
  end

  test "Select where in" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('10', 20, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('40', 50, 'six')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('70', 80, 'nine')")

    expected = [
      %{id1: "10", id2: 20, data: "three", map: nil, transformed: ""},
      %{id1: "40", id2: 50, data: "six", map: nil, transformed: ""},
      %{id1: "70", id2: 80, data: "nine", map: nil, transformed: ""},
    ]

    actual =
      TestTable
      |> select(:all)
      |> where(id1: [in: ["10", "40", "70"]])
      |> TestTable.all

    assert(actual === {:ok, expected})
  end

  test "Select where in prepared/2" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('100', 200, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('400', 500, 'six')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('700', 800, 'nine')")

    expected = [
      %{id1: "100", id2: 200, data: "three", map: nil, transformed: ""},
      %{id1: "400", id2: 500, data: "six", map: nil, transformed: ""},
      %{id1: "700", id2: 800, data: "nine", map: nil, transformed: ""},
    ]

    actual =
      TestTable
      |> prepared(id1: ["100", "400", "700"])
      |> select(:all)
      |> where(id1: [in: :id1])
      |> TestTable.all

    assert(actual === {:ok, expected})
  end

  test "Select where in prepared/1" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 2, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('4', 5, 'six')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('7', 8, 'nine')")

    expected = [
      %{id1: "1", id2: 2, data: "three", map: nil, transformed: ""},
      %{id1: "4", id2: 5, data: "six", map: nil, transformed: ""},
      %{id1: "7", id2: 8, data: "nine", map: nil, transformed: ""},
    ]

    actual =
      TestTable
      |> prepared()
      |> select(:all)
      |> where(id1: [in: ["1", "4", "7"]])
      |> TestTable.all

    assert(actual === {:ok, expected})
  end

  test "Select mv" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 2, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('4', 5, 'six')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('7', 8, 'nine')")

    expected = %{id1: "1", id2: 2, data: "three"}

    actual =
      TestView
      |> select(:all)
      |> where(id2: 2)
      |> TestTable.one

    assert(actual === {:ok, expected})
  end

  test "Select transformed" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data, transformed) values ('1', 2, 'three', 3)")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data, transformed) values ('4', 5, 'six', 6)")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data, transformed) values ('7', 8, 'nine', 9)")

    expected = [
      %{id1: "1", id2: 2, data: "three", map: nil, transformed: "3"},
      %{id1: "4", id2: 5, data: "six", map: nil, transformed: "6"},
      %{id1: "7", id2: 8, data: "nine", map: nil, transformed: "9"},
    ]

    {:ok, actual} =
      TestTable
      |> prepared()
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(actual, fn x -> x[:id1] end) === expected)
  end

  test "Insert" do
    inserted = %{id1: "10", id2: 20, data: "data!", map: nil, transformed: nil}
    expected = %{id1: "10", id2: 20, data: "data!", map: nil, transformed: ""}
    {:ok, :success} =
      TestTable
      |> insert(Enum.to_list(inserted))
      |> TestTable.save

    actual =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(actual === {:ok, [expected]})
  end

  test "Insert batch" do
    inserted = [
      %{id1: "10", id2: 10, data: "data!", map: nil, transformed: nil},
      %{id1: "20", id2: 20, data: "data!!", map: nil, transformed: nil},
      %{id1: "30", id2: 30, data: "data!!!", map: nil, transformed: nil},
    ]
    expected = [
      %{id1: "10", id2: 10, data: "data!", map: nil, transformed: ""},
      %{id1: "20", id2: 20, data: "data!!", map: nil, transformed: ""},
      %{id1: "30", id2: 30, data: "data!!!", map: nil, transformed: ""},
    ]
    {:ok, :success} =
      inserted
      |> Enum.map(fn map -> TestTable |> insert(Enum.to_list(map)) end)
      |> TestTable.batch_execute

    {:ok, actual} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(actual, fn r -> r[:id1] end) === expected)
  end

  test "Insert batch prepared/1" do
    inserted = [
      %{id1: "40", id2: 40, data: "data!", map: nil, transformed: nil},
      %{id1: "50", id2: 50, data: "data!!", map: nil, transformed: nil},
      %{id1: "60", id2: 60, data: "data!!!", map: nil, transformed: nil},
    ]
    expected = [
      %{id1: "40", id2: 40, data: "data!", map: nil, transformed: ""},
      %{id1: "50", id2: 50, data: "data!!", map: nil, transformed: ""},
      %{id1: "60", id2: 60, data: "data!!!", map: nil, transformed: ""},
    ]
    {:ok, :success} =
      inserted
      |> Enum.map(fn map ->
        TestTable
        |> prepared(id1: map[:id1], id2: map[:id2], data: map[:data])
        |> insert(id1: :id1, id2: :id2, data: :data)
      end)
      |> TestTable.batch_execute

    {:ok, actual} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(actual, fn r -> r[:id1] end) === expected)
  end

  test "Insert batch prepared/2" do
    inserted = [
      %{id1: "70", id2: 70, data: "data!", map: nil, transformed: nil},
      %{id1: "80", id2: 80, data: "data!!", map: nil, transformed: nil},
      %{id1: "90", id2: 90, data: "data!!!", map: nil, transformed: nil},
    ]
    expected = [
      %{id1: "70", id2: 70, data: "data!", map: nil, transformed: ""},
      %{id1: "80", id2: 80, data: "data!!", map: nil, transformed: ""},
      %{id1: "90", id2: 90, data: "data!!!", map: nil, transformed: ""},
    ]
    {:ok, :success} =
      inserted
      |> Enum.map(fn map ->
        TestTable
        |> prepared()
        |> insert(Enum.to_list(map))
      end)
      |> TestTable.batch_execute

    {:ok, actual} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(actual, fn r -> r[:id1] end) === expected)
  end

  test "Insert batch too large" do
    inserted =
      100..1000
      |> Enum.map(fn id ->
        %{id1: Kernel.to_string(id), id2: id, data: "data!", map: nil, transformed: nil}
      end)
     actual =
      inserted
      |> Enum.map(fn map ->
        TestTable
        |> insert(Enum.to_list(map))
      end)
      |> TestTable.batch_execute

    assert(actual === {:error, "Batch too large"})
  end

  test "query options missing a consistency should get set a default one" do
    validate_consistency =
      fn query_type, default_consistency ->
        Application.put_env(:triton, :read_consistency, default_consistency)
        Application.put_env(:triton, :write_consistency, default_consistency)

        empty_options = []
        result = Triton.Executor.set_consistency(empty_options, query_type)
        expected_result = [consistency: default_consistency]
        assert result == expected_result
      end

    validate_consistency.(:select, :quorum)
    validate_consistency.(:count, :quorum)
    validate_consistency.(:insert, :quorum)
    validate_consistency.(:update, :quorum)
    validate_consistency.(:delete, :quorum)
  end

  test "query options with passed in consistency should remain" do
    validate_incoming_consistency_remains  =
      fn query_type, incoming_consistency, default_consistency ->
        Application.put_env(:triton, :read_consistency, default_consistency)
        Application.put_env(:triton, :write_consistency, default_consistency)

        options = [consistency: incoming_consistency]
        result = Triton.Executor.set_consistency(options, query_type)
        expected_result = [consistency: incoming_consistency]
        assert result == expected_result
      end

    incoming_consistency = :quorum
    default_consistency = :one
    validate_incoming_consistency_remains.(:select, incoming_consistency, default_consistency)
    validate_incoming_consistency_remains.(:count, incoming_consistency, default_consistency)
    validate_incoming_consistency_remains.(:insert, incoming_consistency, default_consistency)
    validate_incoming_consistency_remains.(:update, incoming_consistency, default_consistency)
    validate_incoming_consistency_remains.(:delete, incoming_consistency, default_consistency)
  end
end
