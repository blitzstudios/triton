defmodule Triton.APM.Noop do
  alias Triton.APM
  @behaviour APM

  @impl APM
  def record(apm = %APM{}), do: :ok

  @impl APM
  def count_event(_event, _labels), do: :ok
end
