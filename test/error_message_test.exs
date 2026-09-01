defmodule Triton.Executor.ErrorMessageTests do
  use ExUnit.Case, async: true

  alias Triton.Executor

  # A non-exception struct can still export message/1, and Exception.message/1 requires
  # __exception__: true, so dispatching on function_exported? raised FunctionClauseError.
  defmodule NotAnException do
    defstruct [:reason]
    def message(_), do: "not an exception"
  end

  defp conn_error(reason, action \\ "execute") do
    %Xandra.ConnectionError{action: action, reason: reason}
  end

  # The contract every caller depends on: the generated all/stream/count/save/del/
  # batch_execute functions return {:error, binary}, so this must ALWAYS return a
  # binary and must NEVER raise, whatever it is handed.
  @every_shape [
    # Xandra.ConnectionError — no :message field at all, the original KeyError source
    {"conn error, checkout refusal", %Xandra.ConnectionError{action: "check out connection", reason: :too_many_concurrent_requests}},
    {"conn error, closed", %Xandra.ConnectionError{action: "execute", reason: :closed}},
    {"conn error, disconnected", %Xandra.ConnectionError{action: "execute", reason: :disconnected}},
    {"conn error, not_connected", %Xandra.ConnectionError{action: "execute", reason: :not_connected}},
    {"conn error, cluster tuple", %Xandra.ConnectionError{action: "checkout", reason: {:cluster, :not_connected}}},
    {"conn error, timeout", %Xandra.ConnectionError{action: "prepare", reason: :timeout}},
    {"conn error, posix", %Xandra.ConnectionError{action: "connect", reason: :econnrefused}},
    {"conn error, crashed tuple", %Xandra.ConnectionError{action: "execute", reason: {:connection_process_crashed, :killed}}},
    # unrecognized tuple reason trips :inet.format_error inside Xandra's catch-all
    {"conn error, unknown tuple", %Xandra.ConnectionError{action: "execute", reason: {:unexpected, :thing}}},
    {"conn error, nil reason", %Xandra.ConnectionError{action: "execute", reason: nil}},

    # Xandra.Error — has :message, so it must pass through untouched
    {"xandra error, binary msg", %Xandra.Error{reason: :invalid, message: "boom"}},
    {"xandra error, nil msg", %Xandra.Error{reason: :invalid, message: nil}},
    {"xandra error, atom msg", %Xandra.Error{reason: :invalid, message: :oops}},
    {"xandra error, tuple msg", %Xandra.Error{reason: :invalid, message: {:a, :b}}},

    # Triton.Error — defstruct, NOT an exception, binary message (validation path)
    {"triton error", %Triton.Error{message: "Invalid input. name must be present."}},
    {"triton error, empty msg", %Triton.Error{message: ""}},

    # other exceptions that could reach a {:error, _} branch
    {"runtime error", %RuntimeError{message: "nope"}},
    {"argument error", %ArgumentError{message: "bad arg"}},
    {"key error", %KeyError{key: :message, term: %{}}},
    {"match error", %MatchError{term: :x}},
    {"function clause error", %FunctionClauseError{module: Foo, function: :bar, arity: 1}},
    {"protocol undefined", %Protocol.UndefinedError{protocol: Enumerable, value: 1, description: ""}},

    # non-exception structs
    {"URI struct", %URI{}},

    # plain maps — the execute_error/1 shape and degenerate variants
    {"map with binary msg", %{message: "Your operation was not applied."}},
    {"map with nil msg", %{message: nil}},
    {"map with atom msg", %{message: :nope}},
    {"empty map", %{}},
    {"map, no msg key", %{reason: :whatever}},

    # bare terms
    {"atom", :boom},
    {"nil", nil},
    {"tuple", {:cluster, :not_connected}},
    {"list", [:a, :b]},
    {"charlist", ~c"oops"},
    {"integer", 42},
    {"float", 1.5},
    {"binary", "already a string"},
    {"empty binary", ""},
    {"nested", %{a: [%{b: {:c, [1, 2]}}]}},

    # a bare map carrying __exception__ is NOT an exception; Exception.message/1
    # requires a struct, so a loose %{} match here raised FunctionClauseError
    {"bare map claiming exception", %{__exception__: true}},
    {"bare map, exception + msg", %{__exception__: true, message: nil}}
  ]

  for {label, input} <- @every_shape do
    test "never raises and returns a binary: #{label}" do
      input = unquote(Macro.escape(input))
      result = Executor.error_message(input)
      assert is_binary(result), "expected a binary, got: #{inspect(result)}"
    end
  end

  test "a non-exception struct exporting message/1 does not raise" do
    result = Executor.error_message(struct(NotAnException, reason: :x))
    assert is_binary(result)
    assert result =~ "NotAnException"
  end

  # Stability: anything that already had a usable message keeps the exact same string,
  # so existing error text does not change.
  test "an existing binary :message passes through byte-identical" do
    assert Executor.error_message(%Xandra.Error{reason: :invalid, message: "boom"}) == "boom"
    assert Executor.error_message(%Triton.Error{message: "Invalid input."}) == "Invalid input."
    assert Executor.error_message(%{message: "Your operation was not applied."}) ==
             "Your operation was not applied."
    assert Executor.error_message(%RuntimeError{message: "nope"}) == "nope"
  end

  test "a ConnectionError names both the action and the reason" do
    message = Executor.error_message(conn_error(:too_many_concurrent_requests, "check out connection"))

    assert message =~ "check out connection"
    assert message =~ "too many requests in flight"
  end

  test "unrecognized terms are inspected rather than raising" do
    assert Executor.error_message({:cluster, :not_connected}) == "{:cluster, :not_connected}"
    assert Executor.error_message(:oops) == ":oops"
    assert Executor.error_message(nil) == "nil"
  end
end
