defmodule Triton.Paging.Tests do
  use ExUnit.Case, async: false

  alias Triton.Executor

  @refusal {:error, %Xandra.ConnectionError{
              action: "check out connection",
              reason: :too_many_concurrent_requests
            }}

  setup do
    {:ok, calls} = Agent.start_link(fn -> [] end)
    on_exit(fn -> Application.delete_env(:triton, :connection_retry_attempts) end)
    {:ok, calls: calls}
  end

  defp page(rows, paging_state) do
    {:ok,
     %Xandra.Page{
       columns: [
         {"triton_tests", "test_table", "id1", :text},
         {"triton_tests", "test_table", "id2", :bigint}
       ],
       content: rows,
       paging_state: paging_state
     }}
  end

  # Records the options each attempt was called with, and returns the next scripted result.
  # The last entry repeats, so a script can end in a permanent failure.
  defp fetching(calls, results) do
    {:ok, remaining} = Agent.start_link(fn -> results end)

    fn options ->
      Agent.update(calls, &(&1 ++ [options]))

      Agent.get_and_update(remaining, fn
        [only] -> {only, [only]}
        [head | tail] -> {head, tail}
      end)
    end
  end

  defp attempts(calls), do: Agent.get(calls, &length(&1))
  defp options(calls), do: Agent.get(calls, & &1)

  test "a single page returns its rows with atom keys", %{calls: calls} do
    fetch = fetching(calls, [page([["a", 1], ["b", 2]], nil)])

    assert {:ok, [%{id1: "a", id2: 1}, %{id1: "b", id2: 2}]} = Executor.fetch_pages([], fetch)
    assert attempts(calls) == 1
  end

  test "pages are concatenated in order and thread the paging state", %{calls: calls} do
    fetch =
      fetching(calls, [
        page([["a", 1]], "after-1"),
        page([["b", 2]], "after-2"),
        page([["c", 3]], nil)
      ])

    assert {:ok, rows} = Executor.fetch_pages([consistency: :one], fetch)
    assert Enum.map(rows, & &1[:id1]) == ["a", "b", "c"]
    assert attempts(calls) == 3

    assert [first, second, third] = options(calls)
    refute Keyword.has_key?(first, :paging_state)
    assert second[:paging_state] == "after-1"
    assert third[:paging_state] == "after-2"
    assert Enum.all?(options(calls), & &1[:consistency] == :one)
  end

  # The regression: this raised MatchError before, because the recursive page fetch was
  # consumed with {:ok, results} = ...
  test "a refusal on page 2 returns an error instead of raising", %{calls: calls} do
    Application.put_env(:triton, :connection_retry_attempts, 3)
    fetch = fetching(calls, [page([["a", 1]], "after-1"), @refusal])

    assert @refusal = Executor.fetch_pages([], fetch)
    assert attempts(calls) == 4
  end

  test "a refused page is retried from its paging state, keeping earlier pages", %{calls: calls} do
    Application.put_env(:triton, :connection_retry_attempts, 3)

    fetch =
      fetching(calls, [
        page([["a", 1]], "after-1"),
        @refusal,
        page([["b", 2]], nil)
      ])

    assert {:ok, rows} = Executor.fetch_pages([], fetch)
    assert Enum.map(rows, & &1[:id1]) == ["a", "b"]
    assert attempts(calls) == 3

    # Page 1 was not re-fetched: the retry re-sent page 2's paging state.
    assert [_first, refused, retried] = options(calls)
    assert refused[:paging_state] == "after-1"
    assert retried[:paging_state] == "after-1"
  end

  test "an error on page 1 is still returned", %{calls: calls} do
    Application.put_env(:triton, :connection_retry_attempts, 2)
    fetch = fetching(calls, [@refusal])

    assert @refusal = Executor.fetch_pages([], fetch)
    assert attempts(calls) == 2
  end

  test "a query error on page 2 is returned without retrying", %{calls: calls} do
    error = {:error, %Xandra.Error{reason: :invalid, message: "boom"}}
    fetch = fetching(calls, [page([["a", 1]], "after-1"), error])

    assert ^error = Executor.fetch_pages([], fetch)
    assert attempts(calls) == 2
  end

  test "a non-refusal connection error on page 2 is returned without retrying", %{calls: calls} do
    error = {:error, %Xandra.ConnectionError{action: "execute", reason: :closed}}
    fetch = fetching(calls, [page([["a", 1]], "after-1"), error])

    assert ^error = Executor.fetch_pages([], fetch)
    assert attempts(calls) == 2
  end
end
