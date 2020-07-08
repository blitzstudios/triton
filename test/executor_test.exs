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

  defmodule TestTable do
    use Triton.Table
    import Triton.Query

    table :test_table, [keyspace: TestKeyspace] do
      field :id1, :text
      field :id2, :bigint
      field :data, :text
      field :map, {:map, "<int, text>"}
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

    expected = %{id1: "1", id2: 2, data: "three", map: nil}

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
      %{id1: "10", id2: 20, data: "three", map: nil},
      %{id1: "40", id2: 50, data: "six", map: nil},
      %{id1: "70", id2: 80, data: "nine", map: nil},
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
      %{id1: "100", id2: 200, data: "three", map: nil},
      %{id1: "400", id2: 500, data: "six", map: nil},
      %{id1: "700", id2: 800, data: "nine", map: nil},
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
      %{id1: "1", id2: 2, data: "three", map: nil},
      %{id1: "4", id2: 5, data: "six", map: nil},
      %{id1: "7", id2: 8, data: "nine", map: nil},
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

  test "Insert" do
    expected = %{id1: "10", id2: 20, data: "data!", map: nil}
    {:ok, :success} =
      TestTable
      |> insert(Enum.to_list(expected))
      |> TestTable.save

    inserted =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(inserted === {:ok, [expected]})
  end

  test "Insert batch" do
    expected = [
      %{id1: "10", id2: 10, data: "data!", map: nil},
      %{id1: "20", id2: 20, data: "data!!", map: nil},
      %{id1: "30", id2: 30, data: "data!!!", map: nil},
    ]
    {:ok, :success} =
      expected
      |> Enum.map(fn map -> TestTable |> insert(Enum.to_list(map)) end)
      |> TestTable.batch_execute

    {:ok, inserted} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(inserted, fn r -> r[:id1] end) === expected)
  end

  test "Insert batch prepared/1" do
    expected = [
      %{id1: "40", id2: 40, data: "data!", map: nil},
      %{id1: "50", id2: 50, data: "data!!", map: nil},
      %{id1: "60", id2: 60, data: "data!!!", map: nil},
    ]
    {:ok, :success} =
      expected
      |> Enum.map(fn map ->
        TestTable
        |> prepared(id1: map[:id1], id2: map[:id2], data: map[:data])
        |> insert(id1: :id1, id2: :id2, data: :data)
      end)
      |> TestTable.batch_execute

    {:ok, inserted} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(inserted, fn r -> r[:id1] end) === expected)
  end

  test "Insert batch prepared/2" do
    expected = [
      %{id1: "70", id2: 70, data: "data!", map: nil},
      %{id1: "80", id2: 80, data: "data!!", map: nil},
      %{id1: "90", id2: 90, data: "data!!!", map: nil},
    ]
    {:ok, :success} =
      expected
      |> Enum.map(fn map ->
           TestTable
           |> prepared()
           |> insert(Enum.to_list(map))
         end)
      |> TestTable.batch_execute

    {:ok, inserted} =
      TestTable
      |> select(:all)
      |> TestTable.all

    assert(Enum.sort_by(inserted, fn r -> r[:id1] end) === expected)
  end
end
