defmodule Triton do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = for config <- Application.get_env(:triton, :clusters, []), into: [] do
      supervisor(Triton.Supervisor, [config], [id: make_ref()])
    end

    Supervisor.start_link(children, [strategy: :one_for_one, name: Triton.Application])
  end
end
