defmodule Triton.Retry.Tests.EchoAPM do
  @behaviour Triton.APM
  def record(_apm), do: :ok
  def count_event(event, labels) do
    send(:apm_echo, {event, labels})
    :ok
  end
end

# Predates count_event/2 — must be skipped, not crashed, so Triton can ship ahead of the app.
defmodule Triton.Retry.Tests.LegacyAPM do
  def record(_apm), do: :ok
end

defmodule Triton.Retry.Tests.BoomAPM do
  def record(_apm), do: :ok
  def count_event(_event, _labels), do: raise("boom")
end

defmodule Triton.Retry.Tests do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Triton.Retry

  @refusal {:error, %Xandra.ConnectionError{
              action: "check out connection",
              reason: :too_many_concurrent_requests
            }}

  setup do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> Application.delete_env(:triton, :connection_retry_attempts) end)
    {:ok, counter: counter}
  end

  defp count(counter), do: Agent.get(counter, & &1)

  defp returning(counter, results) do
    {:ok, remaining} = Agent.start_link(fn -> results end)

    fn ->
      Agent.update(counter, & &1 + 1)
      Agent.get_and_update(remaining, fn
        [only] -> {only, [only]}
        [head | tail] -> {head, tail}
      end)
    end
  end

  test "a successful call runs once", %{counter: counter} do
    assert {:ok, :success} = Retry.on_checkout_refused(returning(counter, [{:ok, :success}]))
    assert count(counter) == 1
  end

  test "a checkout refusal is retried up to the attempt limit", %{counter: counter} do
    Application.put_env(:triton, :connection_retry_attempts, 3)

    assert @refusal = Retry.on_checkout_refused(returning(counter, [@refusal]))
    assert count(counter) == 3
  end

  test "a refusal followed by success returns the success", %{counter: counter} do
    assert {:ok, :success} =
             Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))

    assert count(counter) == 2
  end

  test "other connection errors are not retried", %{counter: counter} do
    other = {:error, %Xandra.ConnectionError{action: "execute", reason: :closed}}

    assert ^other = Retry.on_checkout_refused(returning(counter, [other]))
    assert count(counter) == 1
  end

  test "query errors are not retried", %{counter: counter} do
    error = {:error, %Xandra.Error{reason: :invalid, message: "boom"}}

    assert ^error = Retry.on_checkout_refused(returning(counter, [error]))
    assert count(counter) == 1
  end

  test "exhausting every attempt logs at :error so it reaches Sentry", %{counter: counter} do
    Application.put_env(:triton, :connection_retry_attempts, 2)

    log = capture_log(fn -> assert @refusal = Retry.on_checkout_refused(returning(counter, [@refusal])) end)

    assert log =~ "[error]"
    assert log =~ "refused after 2 attempt(s)"
    assert count(counter) == 2
  end

  test "a retry that succeeds does not log at :error", %{counter: counter} do
    log =
      capture_log(fn ->
        assert {:ok, :success} =
                 Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))
      end)

    refute log =~ "[error]"
    assert log =~ "[warning]"
  end

  describe "retry delay" do
    setup do
      on_exit(fn -> Application.delete_env(:triton, :connection_retry_jitter_ms) end)
      :ok
    end

    test "the first retry is immediate" do
      # remaining == total means no attempt has been consumed yet, so this is retry #1.
      assert Retry.retry_delay_ms(3, 3) == 0
    end

    test "later retries wait a random interval within the configured jitter" do
      Application.put_env(:triton, :connection_retry_jitter_ms, 50)

      delays = for _ <- 1..200, do: Retry.retry_delay_ms(2, 3)

      assert Enum.all?(delays, & &1 >= 1 and &1 <= 50)
      # Randomized, not a fixed pause: a constant would collapse to one distinct value and
      # move the whole herd intact rather than spreading it.
      assert length(Enum.uniq(delays)) > 10
    end

    test "jitter of 0 disables the wait entirely" do
      Application.put_env(:triton, :connection_retry_jitter_ms, 0)

      assert Retry.retry_delay_ms(2, 3) == 0
      assert Retry.retry_delay_ms(1, 3) == 0
    end

    test "a nonsense jitter setting falls back to the default" do
      Application.put_env(:triton, :connection_retry_jitter_ms, "soon")
      assert Retry.jitter_ms() == 25

      Application.put_env(:triton, :connection_retry_jitter_ms, -5)
      assert Retry.jitter_ms() == 25
    end

    test "exhausting attempts actually sleeps between the later ones", %{counter: counter} do
      Application.put_env(:triton, :connection_retry_attempts, 3)
      Application.put_env(:triton, :connection_retry_jitter_ms, 40)

      {elapsed_us, _} =
        :timer.tc(fn -> Retry.on_checkout_refused(returning(counter, [@refusal])) end)

      # Three attempts: retry 1 immediate, retry 2 sleeps 1..40ms. Asserting only the lower
      # bound, since the actual value is random by design.
      assert count(counter) == 3
      assert elapsed_us >= 1_000
      assert elapsed_us < 200_000
    end

    test "a first-retry success is not delayed", %{counter: counter} do
      Application.put_env(:triton, :connection_retry_jitter_ms, 500)

      {elapsed_us, result} =
        :timer.tc(fn ->
          Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))
        end)

      assert result == {:ok, :success}
      # The common case — one hot connection, siblings idle — must not pay the jitter.
      assert elapsed_us < 100_000
    end
  end

  describe "refusal counters" do
    setup do
      Process.register(self(), :apm_echo)
      Application.put_env(:triton, :apm_module, Triton.Retry.Tests.EchoAPM)
      on_exit(fn -> Application.delete_env(:triton, :apm_module) end)
      :ok
    end

    test "a retry that succeeds counts outcome=retried and never exhausted", %{counter: counter} do
      assert {:ok, :success} =
               Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))

      assert_received {:checkout_refused, %{outcome: "retried"}}
      refute_received {:checkout_refused, %{outcome: "exhausted"}}
    end

    test "exhausting attempts counts one exhausted after the retries", %{counter: counter} do
      Application.put_env(:triton, :connection_retry_attempts, 3)

      assert @refusal = Retry.on_checkout_refused(returning(counter, [@refusal]))

      assert_received {:checkout_refused, %{outcome: "retried"}}
      assert_received {:checkout_refused, %{outcome: "retried"}}
      assert_received {:checkout_refused, %{outcome: "exhausted"}}
      refute_received {:checkout_refused, _}
    end

    test "a success counts nothing", %{counter: counter} do
      assert {:ok, :success} = Retry.on_checkout_refused(returning(counter, [{:ok, :success}]))

      refute_received {:checkout_refused, _}
    end
  end

  describe "APM implementations that cannot count" do
    test "an implementation without count_event/2 is skipped, not crashed", %{counter: counter} do
      Application.put_env(:triton, :apm_module, Triton.Retry.Tests.LegacyAPM)
      on_exit(fn -> Application.delete_env(:triton, :apm_module) end)

      assert {:ok, :success} =
               Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))
    end

    test "a raising implementation does not break the retry", %{counter: counter} do
      Application.put_env(:triton, :apm_module, Triton.Retry.Tests.BoomAPM)
      on_exit(fn -> Application.delete_env(:triton, :apm_module) end)

      assert {:ok, :success} =
               Retry.on_checkout_refused(returning(counter, [@refusal, {:ok, :success}]))
    end
  end

  test "attempts is clamped to a sane value" do
    Application.put_env(:triton, :connection_retry_attempts, 0)
    assert Retry.attempts() == 3

    Application.put_env(:triton, :connection_retry_attempts, "nope")
    assert Retry.attempts() == 3

    Application.put_env(:triton, :connection_retry_attempts, 5)
    assert Retry.attempts() == 5
  end
end
