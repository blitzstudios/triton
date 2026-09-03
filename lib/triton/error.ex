defmodule Triton.Error do
  defstruct message: ""

  @doc """
  Renders any error Triton might surface as the string its public functions return.

  Lives here rather than in `Triton.Executor` because `Triton.Validate` needs it too, and a
  validation module reaching into the executor for string formatting is the wrong dependency.

  `Xandra.ConnectionError` is `defexception [:action, :reason]` with no `:message` field, so
  reading `err.message` raises `KeyError` and masks the real error. Structs that already carry
  a binary `:message` (including `%Triton.Error{}`) are returned unchanged so existing strings
  are stable.
  """
  def message(%{message: message}) when is_binary(message), do: message
  def message(error) when is_exception(error) do
    Exception.message(error)
  rescue
    _ -> inspect(error)
  catch
    _kind, _reason -> inspect(error)
  end
  def message(other), do: inspect(other)

  def invalid_cql_operation do
    %Triton.Error{message: "Invalid CQL operation.  Must be one of SELECT, INSERT, UPDATE, or DELETE"}
  end

  def vex_error([{:error, field, _, message} | _]) do
    %Triton.Error{message: "Invalid input. #{field} #{message}."}
  end
  def vex_error(_), do: %Triton.Error{message: "Invalid input."}
end
