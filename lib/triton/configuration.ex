defmodule Triton.Configuration do
  def disable_compilation_migrations?(), do: enabled?(:disable_compilation_migrations)

  def enabled?(key) do
    case Application.get_env(:triton, key) do
      true -> true
      "true" -> true
      _ -> false
    end
  end
end