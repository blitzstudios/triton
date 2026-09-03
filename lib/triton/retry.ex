defmodule Triton.Retry do
  @moduledoc """
  Retries a cluster operation when Xandra refuses the connection checkout with
  `:too_many_concurrent_requests`.

  Xandra draws one connection at random per checkout
  (`Xandra.Cluster.ConnectionPool.checkout/1`) and does not fail over, so a single
  connection sitting at `max_concurrent_requests_per_connection` refuses queries while
  its siblings are idle. Retrying re-enters the cluster checkout, which draws a fresh
  host and a fresh connection, so an attempt lands elsewhere with high probability.

  This is safe for writes: the refusal carries `action: "check out connection"`, meaning
  nothing was written to the socket, so a retried statement cannot be applied twice.

  Only checkout refusals are retried. Query errors, timeouts and every other
  `Xandra.ConnectionError` reason are returned to the caller untouched.
  """

  require Logger

  @default_attempts 3
  @default_jitter_ms 25

  @doc """
  Runs `fun`, retrying while it returns a checkout refusal.

  Attempt count comes from `:connection_retry_attempts` (default #{@default_attempts}),
  read per call so it can be changed on a running node.
  """
  def on_checkout_refused(fun) when is_function(fun, 0) do
    on_checkout_refused(fun, attempts())
  end

  def on_checkout_refused(fun, attempts) when is_function(fun, 0) and is_integer(attempts) do
    run(fun, attempts, attempts)
  end

  # Last attempt: a refusal here means every connection we drew was at capacity, which is a
  # real failure. Logged at :error so it reaches Sentry with text that groups on the cause,
  # unlike the KeyError this used to surface as.
  defp run(fun, remaining, total) when remaining <= 1 do
    case fun.() do
      {:error, %Xandra.ConnectionError{reason: :too_many_concurrent_requests}} = refused ->
        count("exhausted")

        Logger.error(fn ->
          "Triton connection checkout refused after #{total} attempt(s); every connection " <>
            "drawn was at max_concurrent_requests_per_connection"
        end)

        refused

      result ->
        result
    end
  end

  defp run(fun, remaining, total) do
    case fun.() do
      {:error, %Xandra.ConnectionError{reason: :too_many_concurrent_requests}} ->
        count("retried")

        Logger.warning(fn ->
          "Triton retrying after connection checkout refusal, #{remaining - 1} attempt(s) left"
        end)

        case retry_delay_ms(remaining, total) do
          0 -> :ok
          ms -> Process.sleep(ms)
        end

        run(fun, remaining - 1, total)

      result ->
        result
    end
  end

  @doc """
  Milliseconds to wait before the next attempt: 0 before the first retry, a random
  1..`:connection_retry_jitter_ms` before any later one.

  Set `:connection_retry_jitter_ms` to 0 to retry with no delay at all.
  """
  def retry_delay_ms(remaining, total) when remaining >= total, do: 0
  def retry_delay_ms(_remaining, _total) do
    case jitter_ms() do
      0 -> 0
      ms -> :rand.uniform(ms)
    end
  end

  # A counter rather than only a log, because a retry that succeeds is the early warning that
  # connections are being lost, and warn-level logs do not reliably reach our log sinks.
  defp count(outcome) do
    apm_module = Application.get_env(:triton, :apm_module) || Triton.APM.Noop
    Triton.APM.count_event(:checkout_refused, %{outcome: outcome}, apm_module)
  end

  def jitter_ms() do
    case Application.get_env(:triton, :connection_retry_jitter_ms, @default_jitter_ms) do
      n when is_integer(n) and n >= 0 -> n
      _ -> @default_jitter_ms
    end
  end

  def attempts() do
    case Application.get_env(:triton, :connection_retry_attempts, @default_attempts) do
      n when is_integer(n) and n >= 1 -> n
      _ -> @default_attempts
    end
  end
end
