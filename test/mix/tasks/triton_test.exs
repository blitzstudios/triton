defmodule Mix.Tasks.TritonTest do
  use ExUnit.Case

  test "provide a list of available triton mix tasks" do
    Mix.Tasks.Triton.run([])
    assert_received {:mix_shell, :info, ["Triton v" <> _]}
    assert_received {:mix_shell, :info, ["mix triton.gen.keyspace" <> _]}
  end

  test "expects no arguments" do
    assert_raise Mix.Error, fn ->
      Mix.Tasks.Triton.run(["invalid"])
    end
  end
end
