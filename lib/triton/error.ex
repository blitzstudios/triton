defmodule Triton.Error do
  defstruct message: ""

  def invalid_cql_operation do
    %Triton.Error{message: "Invalid CQL operation.  Must be one of SELECT, INSERT, UPDATE, or DELETE"}
  end

  def vex_error([{:error, field, _, message} | _]) do
    %Triton.Error{message: "Invalid input. #{field} #{message}."}
  end
  def vex_error(_), do: %Triton.Error{message: "Invalid input."}
end
