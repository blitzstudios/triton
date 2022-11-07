defmodule Triton.APM.Noop do
  alias Triton.APM
  @behaviour APM

  @impl APM
  def record(apm = %APM{}), do: :ok
end
