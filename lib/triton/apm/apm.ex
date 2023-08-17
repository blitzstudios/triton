defmodule Triton.APM do
  require Logger

  @type t :: %__MODULE__{
    keyspace: String.t(),
    schema: String.t(),
    dml_type: String.t(),
    duration_ms: integer(),
    result_type: :ok | :error | :unknown,
    is_batch: boolean(),
    batch_size: integer(),
    active_connections: integer(),
    waiting_connections: integer(),
  }

  @enforce_keys [:keyspace, :schema, :dml_type, :duration_ms, :result_type]
  defstruct [
    :keyspace,
    :schema,
    :dml_type,
    :duration_ms,
    :result_type,
    :is_batch,
    :batch_size,
    :active_connections,
    :waiting_connections
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

  def from_query!(query, conn, duration_ms, result, batch_size \\ :single_query) do
    result_type =
      case result do
        {:ok, _} -> :ok
        {:error, _} -> :error
        _ -> :unknown
      end

    state = :sys.get_state(conn)
    {active, waiting} = state.pools |> Enum.reduce({0, 0}, fn {_, pool}, {active, waiting} ->
      pool_res = DBConnection.ConnectionPool.get_connection_metrics(pool)
      %{active: active_connections, waiting: waiting_connections} = pool_res
      {active + active_connections, waiting + waiting_connections}
    end)

    %__MODULE__{
      keyspace: keyspace!(query, conn) |> to_string,
      dml_type: Triton.Helper.query_type(query) |> to_string,
      duration_ms: duration_ms,
      schema: query[:__table__] |> to_string,
      result_type: result_type,
      is_batch: batch_size != :single_query,
      batch_size: batch_size == :single_query && 0 || batch_size,
      active_connections: active,
      waiting_connections: waiting
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
