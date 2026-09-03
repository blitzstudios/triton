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

  # Bound to its own single-connection cluster so it can be saturated without touching the
  # cluster every other test in this file uses.
  defmodule SaturatedKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: SaturatedConn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule SaturatedTable do
    use Triton.Table

    table :test_table, [keyspace: SaturatedKeyspace] do
      field :id1, :text
      field :id2, :bigint
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

  defmodule TestViewWhere do
    use Triton.MaterializedView
    import Triton.Query

    materialized_view :test_mv_where, from: TestTable do
      fields [
        :id1, :id2, :data
      ]
      Triton.MaterializedView.where "id2 > 0"
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

  # Scylla exposes its live config; Cassandra has no system.config, so fall back to its
  # documented default rather than failing to read it.
  defp batch_fail_threshold_kb() do
    with {:ok, page} <-
           execute_cql("select value from system.config where name = 'batch_size_fail_threshold_in_kb'"),
         %{"value" => value} <- page |> Enum.to_list() |> List.first(),
         {kb, _} <- Integer.parse(value)
    do
      kb
    else
      _ -> 50
    end
  end

  defp drop_test_keyspace(), do: execute_cql("drop keyspace if exists triton_tests")
  defp truncate_test_table(), do: execute_cql("truncate triton_tests.test_table")
  defp drop_test_table(), do: execute_cql("drop table triton_tests.test_table")
  defp drop_test_view(), do: execute_cql("drop materialized view triton_tests.test_mv")
  defp drop_test_view_where(), do: execute_cql("drop materialized view triton_tests.test_mv_where")
  defp drop_test_view_replicas(_replica = 1), do: execute_cql("drop materialized view triton_tests.test_mv_replicas")
  defp drop_test_view_replicas(replica), do: execute_cql("drop materialized view triton_tests.test_mv_replicas_#{replica}")

  setup do
#    Application.put_env(:triton, :enable_auto_prepare, true)
    # dropping keyspace disconnects us
    # drop_test_keyspace()
#    drop_test_view_where()
#    drop_test_view()
#    drop_test_table()
    Triton.Setup.Keyspace.setup(TestKeyspace)
    Triton.Setup.Table.setup(TestTable)
    Triton.Setup.MaterializedView.setup(TestView)
    Triton.Setup.MaterializedView.setup(TestViewWhere)
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

  # Multi-page selects were only covered for streams; the :select paging loop had no test.
  test "Select paginated" do
    rows = for id2 <- 1..5, do: {"paged", id2, "row-#{id2}"}

    Enum.each(rows, fn {id1, id2, data} ->
      {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('#{id1}', #{id2}, '#{data}')")
    end)

    expected = Enum.map(rows, fn {id1, id2, data} ->
      %{id1: id1, id2: id2, data: data, map: nil, transformed: ""}
    end)

    unprepared =
      TestTable
      |> select(:all)
      |> where(id1: "paged")
      |> TestTable.all(page_size: 1)

    assert unprepared === {:ok, expected}

    prepared =
      TestTable
      |> prepared(id1: "paged")
      |> select(:all)
      |> where(id1: :id1)
      |> TestTable.all(page_size: 1)

    assert prepared === {:ok, expected}
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

  test "Select mv defined with where" do
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 1, 'one')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 2, 'two')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('1', 0, 'three')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('2', -1, 'four')")
    {:ok, _} = execute_cql("insert into triton_tests.test_table(id1, id2, data) values ('3', -2, '4')")

    assert({:ok, %{id1: "1", id2: 1, data: "one"}} == TestViewWhere |> select(:all) |> where(id2: 1) |> TestTable.one)
    assert({:ok, %{id1: "1", id2: 2, data: "two"}} == TestViewWhere |> select(:all) |> where(id2: 2) |> TestTable.one)
    assert({:ok, nil} == TestViewWhere |> select(:all) |> where(id2: 0) |> TestTable.one)
    assert({:ok, nil} == TestViewWhere |> select(:all) |> where(id2: -1) |> TestTable.one)
    assert({:ok, nil} == TestViewWhere |> select(:all) |> where(id2: -2) |> TestTable.one)
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

  # The server rejects a batch over batch_size_fail_threshold_in_kb, and that limit varies by
  # deployment: Cassandra defaults to 50KB, this Scylla is configured to 1024KB. The batch was
  # a hardcoded 901 rows (~88KB), which clears the first limit and not the second, so the test
  # passed or failed depending on which server it met. Sizing it from the server's own setting
  # keeps it meaningful on both.
  test "Insert batch too large" do
    payload = String.duplicate("x", 1_000)
    row_bytes = 1_100
    rows = div(batch_fail_threshold_kb() * 1024, row_bytes) * 2

    inserted =
      1..rows
      |> Enum.map(fn id ->
        %{id1: Kernel.to_string(id), id2: id, data: payload, map: nil, transformed: nil}
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

  # Batch building prepares each statement on the checked-out connection, and every prepare
  # takes a request slot before Xandra's statement cache is consulted, so a saturated
  # connection refuses them. That refusal used to hit `{:ok, statement} = prepare_query(...)`
  # and raise MatchError, which propagated past the retry around batch_execute_on_cluster/3 —
  # Triton.Retry matches on returned values and cannot see an exception.
  #
  # Saturation is driven through Xandra's internal checkout call because it is the only
  # deterministic way to fill a connection's in-flight slots. If that protocol changes, this
  # test fails loudly rather than passing vacuously: either the checkout call itself stops
  # matching {:ok, _state} below, or saturation silently stops working and the batch returns
  # {:ok, :success}, failing the assertion. It never goes green without exercising the refusal.
  test "a batch on saturated connections returns an error instead of raising" do
    {:ok, _} =
      Xandra.Cluster.start_link(
        name: SaturatedConn,
        nodes: ["127.0.0.1"],
        keyspace: "triton_tests",
        pool_size: 1,
        max_concurrent_requests_per_connection: 1,
        sync_connect: 5_000
      )

    # An Xandra cluster outlives the test process that linked it, so it has to be killed
    # explicitly or its saturated connections linger for the rest of the run. Killed rather
    # than Xandra.Cluster.stop/1, which exits with :shutdown and would fail the callback.
    on_exit(fn ->
      Application.delete_env(:triton, :connection_retry_attempts)

      case Process.whereis(SaturatedConn) do
        nil ->
          :ok

        pid ->
          ref = Process.monitor(pid)
          Process.exit(pid, :shutdown)

          receive do
            {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
          after
            5_000 -> :ok
          end
      end
    end)

    Application.put_env(:triton, :connection_retry_attempts, 1)

    batch = fn ->
      for {id1, id2} <- [{"sat1", 1}, {"sat2", 2}] do
        SaturatedTable
        |> prepared(id1: id1, id2: id2)
        |> insert(id1: :id1, id2: :id2)
      end
      |> SaturatedTable.batch_execute
    end

    # Control: the harness itself works while the connections are idle.
    assert batch.() === {:ok, :success}

    # The cluster auto-discovers peers, so pool_size: 1 means one connection *per host*.
    # Every one of them has to be filled or the checkout just lands on an idle sibling.
    conns =
      for _ <- 1..40, do: Xandra.Cluster.run(SaturatedConn, fn conn -> conn end)

    conns = Enum.uniq(conns)
    assert conns != []

    Enum.each(conns, fn conn ->
      req_alias = Process.monitor(conn, alias: :reply_demonitor)
      {:ok, _state} = :gen_statem.call(conn, {:checkout_state_for_next_request, req_alias})
    end)

    assert {:error, message} = batch.()
    assert message =~ "too many requests in flight"
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
