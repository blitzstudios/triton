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
        Logger.warning(fn ->
          "Triton retrying after connection checkout refusal, #{remaining - 1} attempt(s) left"
        end)

        run(fun, remaining - 1, total)

      result ->
        result
    end
  end

  def attempts() do
    case Application.get_env(:triton, :connection_retry_attempts, @default_attempts) do
      n when is_integer(n) and n >= 1 -> n
      _ -> @default_attempts
    end
  end
end
