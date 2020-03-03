defmodule Triton.APM.Noop do
  alias Triton.APM
  @behavior APM

  @impl APM
  def record(apm = %APM{}), do: :ok
end
