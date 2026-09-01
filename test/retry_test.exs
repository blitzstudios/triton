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

  test "attempts is clamped to a sane value" do
    Application.put_env(:triton, :connection_retry_attempts, 0)
    assert Retry.attempts() == 3

    Application.put_env(:triton, :connection_retry_attempts, "nope")
    assert Retry.attempts() == 3

    Application.put_env(:triton, :connection_retry_attempts, 5)
    assert Retry.attempts() == 5
  end
end
