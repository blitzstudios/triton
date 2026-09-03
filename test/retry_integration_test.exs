# Counts the retry outcomes Triton.Retry emits, so a test can assert a retry actually happened
# rather than inferring it from a query merely succeeding.
defmodule Triton.RetryIntegration.EchoAPM do
  def record(_apm), do: :ok
  def count_event(event, labels), do: send(:retry_apm_echo, {event, labels})
end

defmodule Triton.RetryIntegration.Tests do
  use ExUnit.Case, async: false
  @moduletag :integration

  import Triton.Query

  alias Triton.RetryIntegration.EchoAPM

  defmodule Keyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: RetryConn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  defmodule VerifyKeyspace do
    use Triton.Keyspace

    keyspace :triton_tests, conn: TritonTests.Conn do
      with_options [
        replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
      ]
    end
  end

  # A counter is the only non-idempotent write available, so it is the only way to prove a
  # retried write applies exactly once.
  defmodule Counter do
    use Triton.Table

    table :test_counter, [keyspace: Keyspace] do
      field :id1, :text
      field :count, :counter
      partition_key [:id1]
    end
  end

  # Same table, read over the healthy cluster so verification isn't blocked by saturation.
  defmodule CounterVerify do
    use Triton.Table

    table :test_counter, [keyspace: VerifyKeyspace] do
      field :id1, :text
      field :count, :counter
      partition_key [:id1]
    end
  end

  defmodule Rows do
    use Triton.Table

    table :test_table, [keyspace: Keyspace] do
      field :id1, :text
      field :id2, :bigint
      partition_key [:id1]
      cluster_columns [:id2]
    end
  end

  setup do
    # Set up over the configured cluster: RetryConn is started per-test and is not in
    # config/test.exs, so Triton.Setup's cluster lookup would find nil for it.
    Triton.Setup.Table.setup(CounterVerify)

    Process.register(self(), :retry_apm_echo)
    Application.put_env(:triton, :apm_module, EchoAPM)
    # Jitter is covered by unit tests; zero here keeps these from sleeping through every retry.
    Application.put_env(:triton, :connection_retry_jitter_ms, 0)

    on_exit(fn ->
      Application.delete_env(:triton, :apm_module)
      Application.delete_env(:triton, :connection_retry_jitter_ms)
      Application.delete_env(:triton, :connection_retry_attempts)
    end)

    :ok
  end

  # One request slot per connection makes saturation exact rather than racy.
  defp start_cluster!(min_conns \\ 1) do
    {:ok, _} =
      Xandra.Cluster.start_link(
        name: RetryConn,
        nodes: ["127.0.0.1"],
        keyspace: "triton_tests",
        pool_size: 1,
        max_concurrent_requests_per_connection: 1,
        sync_connect: 5_000
      )

    # An Xandra cluster outlives the test process that linked it, so without this the name stays
    # registered and the next test fails with {:error, {:already_started, _}} - and its saturated
    # connections linger for the rest of the run.
    # Killed rather than Xandra.Cluster.stop/1, which exits with :shutdown and would fail the
    # on_exit callback itself.
    on_exit(fn ->
      case Process.whereis(RetryConn) do
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

    await_connections(min_conns)
  end

  # The cluster auto-discovers peers, so the connection count is whatever it found rather than
  # pool_size, and discovery continues after start_link returns (sync_connect only waits for the
  # contact node). Drawing repeatedly is the only way to enumerate them, and polling is the only
  # way to know discovery has caught up.
  defp await_connections(min_conns, attempts_left \\ 50) do
    conns =
      for _ <- 1..40, do: Xandra.Cluster.run(RetryConn, fn conn -> conn end)

    case Enum.uniq(conns) do
      found when length(found) >= min_conns ->
        found

      _found when attempts_left > 0 ->
        Process.sleep(100)
        await_connections(min_conns, attempts_left - 1)

      found ->
        flunk("wanted #{min_conns} connection(s), the cluster only ever discovered #{length(found)}")
    end
  end

  defp saturate!(conns) do
    Enum.each(conns, fn conn ->
      req_alias = Process.monitor(conn, alias: :reply_demonitor)
      {:ok, _state} = :gen_statem.call(conn, {:checkout_state_for_next_request, req_alias})
    end)
  end

  defp drain_echo(acc \\ []) do
    receive do
      {:checkout_refused, labels} -> drain_echo([labels[:outcome] | acc])
    after
      0 -> acc
    end
  end

  test "a refused write retries onto another connection and applies exactly once" do
    conns = start_cluster!(2)
    Application.put_env(:triton, :connection_retry_attempts, 10)

    # Exactly one saturated connection: enough that draws land on it regularly, while leaving
    # the rest healthy so a retry reliably recovers. Saturating all-but-one makes exhaustion a
    # real possibility, since the discovered host count varies.
    saturate!([hd(conns)])

    id = "ctr-#{System.unique_integer([:positive])}"
    increments = 20

    results =
      for _ <- 1..increments do
        Counter |> update(count: "count + 1") |> where(id1: id) |> Counter.save
      end

    assert Enum.all?(results, &(&1 == {:ok, :success})), "some writes failed: #{inspect(results)}"

    outcomes = drain_echo()
    assert "retried" in outcomes, "no retry occurred; the test proved nothing"
    refute "exhausted" in outcomes

    # The point of the test: a checkout refusal happens before anything reaches the socket, so
    # a retried write must not double-apply. A counter is the only write that would show it.
    assert {:ok, %{count: ^increments}} =
             CounterVerify |> select(:all) |> where(id1: id) |> CounterVerify.one
  end

  test "a paginated select recovers per page when a connection is saturated" do
    conns = start_cluster!(2)
    Application.put_env(:triton, :connection_retry_attempts, 10)

    id = "page-#{System.unique_integer([:positive])}"

    for id2 <- 1..20 do
      {:ok, :success} = Rows |> insert(id1: id, id2: id2) |> Rows.save
    end

    saturate!([hd(conns)])

    # page_size: 1 forces one round trip per row, each its own retry unit. Twenty of them makes
    # it near-certain at least one draw hits the saturated connection.
    assert {:ok, rows} =
             Rows |> select(:all) |> where(id1: id) |> Rows.all(page_size: 1)

    assert Enum.map(rows, & &1[:id2]) == Enum.to_list(1..20)
    assert "retried" in drain_echo(), "no retry occurred; the test proved nothing"
  end

  test "when every connection is saturated a select returns an error instead of raising" do
    conns = start_cluster!()
    Application.put_env(:triton, :connection_retry_attempts, 2)
    saturate!(conns)

    assert {:error, message} = Rows |> select(:all) |> where(id1: "nope") |> Rows.all()
    assert message =~ "too many requests in flight"

    outcomes = drain_echo()
    assert "retried" in outcomes
    assert "exhausted" in outcomes
  end

  test "streams bypass the retry entirely" do
    conns = start_cluster!()
    Application.put_env(:triton, :connection_retry_attempts, 5)
    saturate!(conns)

    # The stream is lazy, so building it succeeds even with every connection saturated.
    assert {:ok, stream} = Rows |> select(:all) |> where(id1: "nope") |> Rows.stream()

    # The refusal surfaces during enumeration, in the consuming process, as an exception —
    # which is exactly why Triton.Retry cannot cover streams and deliberately skips them.
    assert_raise Xandra.ConnectionError, fn -> Enum.to_list(stream) end
    assert drain_echo() == [], "a stream must not consume retry attempts"
  end
end
