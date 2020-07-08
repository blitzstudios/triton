defmodule Triton.APM do
  require Logger

  @type t :: %__MODULE__{
    keyspace: String.t(),
    schema: String.t(),
    dml_type: String.t(),
    duration_ms: integer()
  }

  @enforce_keys [:keyspace, :schema, :dml_type, :duration_ms]
  defstruct [
    :keyspace,
    :schema,
    :dml_type,
    :duration_ms
  ]

  @callback record(Triton.APM.t()) :: :ok | {:error, any}

  def record(apm = %Triton.APM{}, implementation) do
    try do
      implementation.record(apm)
    rescue
      err -> {:error, err}
    catch
      ex -> {:error, ex}
      :exit, ex -> {:error, ex}
    end
    |> case do
         {:error, err} ->
           Logger.error("Triton apm record error: #{inspect(err)}")
           :error
         _ -> :ok
       end
  end

  def from_query!(query, conn, duration_ms) do
    %__MODULE__{
      keyspace: keyspace!(query, conn) |> to_string,
      dml_type: Triton.Helper.query_type(query) |> to_string,
      duration_ms: duration_ms,
      schema: query[:__table__] |> to_string
    }
  end

  @doc """
  Measures the execution time of a function in milliseconds.
  Returns {f_execution_millis, f_result}
  """
  def execute(f) do
    :timer.tc(f)
    |> fn {micros, result} -> {Integer.floor_div(micros, 1000), result} end.()
  end

  defp keyspace!(query, conn) do
    schema_module = query[:__schema_module__]
    keyspace = Triton.Metadata.keyspace(schema_module)
    keyspace_conn = Triton.Metadata.conn(schema_module)
    dual_keyspace = Triton.Metadata.secondary_keyspace(schema_module)
    dual_keyspace_conn = Triton.Metadata.secondary_conn(schema_module)

    cond do
      keyspace_conn == conn -> keyspace
      dual_keyspace_conn == conn -> dual_keyspace
      true -> nil
    end
  end
end
