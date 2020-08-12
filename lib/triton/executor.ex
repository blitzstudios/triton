defmodule Triton.Executor do
  require Logger

  defmacro __using__(_) do
    quote do
      def all(query, options \\ []) do
        case Triton.Executor.execute(query, options) do
          {:error, err} -> {:error, err.message}
          {:ok, results} -> {:ok, transform_results(query, results)}
        end
      end

      def stream(query, options \\ []) do
        case Triton.Executor.execute([{:stream, true} | query], options) do
          {:error, err} -> {:error, err.message}
          {:ok, results} -> {:ok, transform_results(query, results)}
        end
      end

      def count(query, options \\ []) do
        case Triton.Executor.execute([{:count, true} | query], options) do
          {:error, err} -> {:error, err.message}
          results -> results
        end
      end

      def one(query, options \\ []) do
        case all(query, options) do
          {:ok, results} -> {:ok, List.first(results)}
          err -> err
        end
      end

      def save(query, options \\ []) do
        case Triton.Executor.execute(query, options) do
          {:error, err} -> {:error, err.message}
          result -> result
        end
      end

      def del(query, options \\ []) do
        case Triton.Executor.execute(query, options) do
          {:error, err} -> {:error, err.message}
          result -> result
        end
      end

      def batch_execute(queries, options \\ []) do
        case Triton.Executor.batch_execute(queries, options) do
          {:ok, results} -> {:ok, results}
          {:error, err} -> {:error, err.message}
        end
      end

      defp transform_results(query, results) when is_list(results) do
        transforms =
          Triton.Metadata.fields(query[:__schema_module__])
          |> Enum.filter(fn {k, field} -> not is_nil(field[:opts][:transform]) end)
          |> Enum.map(fn {k, field} -> {k, field[:opts][:transform]} end)

        case Enum.any?(transforms) do
          false -> results
          true ->
            results
            |> Enum.map(fn result ->
                 transforms
                 |> Enum.reduce(result, fn {k, transform}, acc ->
                      acc |> Map.put(k, transform.(acc[k]))
                    end)
               end)
        end
      end
      defp transform_results(_, results), do: results
    end
  end

  defp batch_execute_on_cluster(cluster, queries, options) do
    cqls =
      queries
      |> Enum.map(fn query ->
        query = Triton.CQL.Parameterize.parameterize!(query)
        {:ok, _, cql} = build_cql(query)
        {cql, query[:prepared]}
      end)

    Xandra.Cluster.run(cluster, fn conn ->
      batch =
        cqls
        |> Enum.reduce(Xandra.Batch.new(), fn ({cql, prepared}, acc) ->
          case prepared do
            nil -> Xandra.Batch.add(acc, cql)
            prepared -> Xandra.Batch.add(acc, Xandra.prepare!(conn, cql, options), atom_to_string_keys(prepared))
          end
        end)

      with {:ok, %Xandra.Void{}} <- Xandra.execute(conn, batch, options),
           do: {:ok, :success}
    end)
  end

  defp batch_dual_execute(primary_result, cluster, queries, options) do
    case primary_result do
      {:ok, _} ->
        try do
          batch_execute_on_cluster(cluster, queries, options)
        rescue
          err -> {:error, err}
        catch
          :exit, err -> {:error, err}
          err -> {:error, err}
        end
        |> case do
             {:error, err} -> Logger.error("Triton batch_execute dual write error: #{inspect(err)}")
             _ -> :noop
           end
      _ -> :noop
    end
  end

  @doc """
  Batch execute, like execute, but on a list of queries
  """
  def batch_execute(queries, options \\ [])
  def batch_execute(queries, options) when is_list(queries) and length(queries) > 0 do
    cluster = List.first(queries) |> cluster_for
    dual_write_cluster = List.first(queries) |> dual_execute_cluster_for
    should_dual_execute? = dual_writes_enabled() && not is_nil(dual_write_cluster)

    cluster_result = batch_execute_on_cluster(cluster, queries, options)

    _ =
      cond do
        should_dual_execute? -> batch_dual_execute(cluster_result, dual_write_cluster, queries, options)
        true -> :noop
      end

    cluster_result
  end
  def batch_execute(_, _), do: {:ok, :success}

  @doc """
  Creates a valid CQL query out of a query keyword list and executes it.
  Returns {:ok, results}
          {:error, error}
  """
  def execute(query, options \\ []) do
    with cluster <- cluster_for(query)
    do
      result = execute_on_cluster(query, cluster, options)
      _ = dual_execute(result, query, options)
      result
    end
  end

  defp dual_execute(primary_result, query, options) do
    dual_execute_cluster = dual_execute_cluster_for(query)
    should_dual_execute? = not is_nil(dual_execute_cluster)
    type = Triton.Helper.query_type(query)

    cond do
      should_dual_execute? && dual_writes_enabled() && type in [:insert, :update, :delete] ->
        dual_write(primary_result, query, dual_execute_cluster, options)
      should_dual_execute? && dual_reads_enabled() && type in [:select] ->
        dual_read(primary_result, query, dual_execute_cluster, options)
      true -> :noop
    end
  end

  defp is_mv_select(query) do
    query[:select] && Triton.Metadata.is_materialized_view(query[:__schema_module__])
  end

  defp dual_write(primary_result, query, cluster, options) do
    case primary_result do
      {:ok, _} ->
        try do
          execute_on_cluster(query, cluster, options)
        rescue
          err -> {:error, err}
        catch
          ex -> {:error, ex}
          :exit, ex -> {:error, ex}
        end
        |> case do
             {:error, err} -> Logger.error(fn -> "Triton execute dual write error: #{inspect(err)}, query: #{inspect(query)}" end)
             _ -> :noop
           end
       _ -> :noop
    end
  end

  defp dual_read(primary_result, query, cluster, options) do
    try do
      dual_read_result = execute_on_cluster(query, cluster, options)

      case {is_mv_select(query), primary_result, dual_read_result} do
        {true, _, _} -> true
        {false, {:ok, primary}, {:ok, secondary}} when is_list(primary) and is_list(secondary) ->
          MapSet.equal?(MapSet.new(primary), MapSet.new(secondary))
        _ -> primary_result == dual_read_result
      end
      |> case do
           true -> :noop
           false ->
             Logger.error(fn -> "Triton execute dual read mismatch, query: #{inspect(query)}" end)
             :noop
         end

      dual_read_result
    rescue
      err -> {:error, err}
    catch
      ex -> {:error, ex}
      :exit, ex -> {:error, ex}
    end
    |> case do
         {:error, err} -> Logger.error(fn -> "Triton execute dual read error: #{inspect(err)}, query: #{inspect(query)}" end)
         _ -> :noop
       end
  end

  def execute_on_cluster(query, cluster, options \\ []) do
    apm_module = Application.get_env(:triton, :apm_module) || Triton.APM.Noop
    with {:ok, query} <- Triton.Validate.coerce(query),
         query <- query |> auto_prepare |> Triton.CQL.Parameterize.parameterize!,
         {:ok, type, cql} <- build_cql(query),
         exec_fn = fn () -> execute_cql(cluster, type, cql, query[:prepared], options) end,
         {duration_ms, result} = Triton.APM.execute(exec_fn),
         _ = Triton.APM.from_query!(query, cluster, duration_ms, result)
             |> Triton.APM.record(apm_module)
    do
      _ = case result do
        {:error, err} -> Logger.error(fn -> "Triton primary execute error: #{inspect(err)}, query: #{inspect(query)}" end)
        _ -> :noop
      end

      result
    end
  end

  defp build_cql(query) do
    case Triton.Helper.query_type(query) do
      :stream -> {:ok, :stream, Triton.CQL.Select.build(query)}
      :count  -> {:ok, :count, Triton.CQL.Select.build(query)}
      :select -> {:ok, :select, Triton.CQL.Select.build(query)}
      :insert -> {:ok, :insert, Triton.CQL.Insert.build(query)}
      :update -> {:ok, :update, Triton.CQL.Update.build(query)}
      :delete -> {:ok, :delete, Triton.CQL.Delete.build(query)}
      err -> err
    end
  end

  defp execute_cql(cluster, :stream, cql, nil, options) do
    with pages <- Xandra.Cluster.stream_pages!(cluster, cql, [], options) do
      results = pages
        |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
      {:ok, results}
    end
  end
  defp execute_cql(cluster, :stream, cql, prepared, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      with {:ok, statement} <- Xandra.prepare(conn, cql, options),
           pages <- Xandra.stream_pages!(conn, statement, atom_to_string_keys(prepared), options)
      do
        results = pages
          |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
        {:ok, results}
      end
    end)
  end

  defp execute_cql(cluster, :select, cql, nil, options) do
    with {:ok, page} <- Xandra.Cluster.execute(cluster, cql, [], options),
      formatted_page = Enum.to_list(page) |> format_results
    do
      case page.paging_state do
        nil -> {:ok, formatted_page}
        paging_state ->
          {:ok, results} = execute_cql(cluster, :select, cql, nil, Keyword.put(options, :paging_state, paging_state))
          {:ok, formatted_page ++ results}
      end
    end
  end
  defp execute_cql(cluster, :select, cql, prepared, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      with {:ok, statement} <- Xandra.prepare(conn, cql, Keyword.delete(options, :paging_state)),
        {:ok, page} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), options),
        formatted_page = Enum.to_list(page) |> format_results
      do
        case page.paging_state do
          nil -> {:ok, formatted_page}
          paging_state ->
            {:ok, results} = execute_cql(cluster, :select, cql, prepared, Keyword.put(options, :paging_state, paging_state))
            {:ok, formatted_page ++ results}
        end
      end
    end)
  end

  defp execute_cql(cluster, :count, cql, nil, options) do
    with {:ok, page} <- Xandra.Cluster.execute(cluster, cql, [], options),
         count <- page |> Enum.to_list |> List.first |> Map.get("count"),
      do: {:ok, count}
  end
  defp execute_cql(cluster, :count, cql, prepared, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      with {:ok, statement} <- Xandra.prepare(conn, cql, options),
           {:ok, page} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), options),
           count <- page |> Enum.to_list |> List.first |> Map.get("count"),
        do: {:ok, count}
    end)
  end

  defp execute_cql(cluster, _, cql, nil, options) do
    with {:ok, %Xandra.Void{}} <- Xandra.Cluster.execute(cluster, cql, [], options) do
      {:ok, :success}
    else
      error -> error |> execute_error
    end
  end
  defp execute_cql(cluster, _, cql, prepared, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      with {:ok, statement} <- Xandra.prepare(conn, cql, options),
           {:ok, %Xandra.Void{}} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), options)
      do
        {:ok, :success}
      else
        error -> error |> execute_error
      end
    end)
  end

  defp execute_error({:ok, %Xandra.Page{} = page}) do
    case page |> Enum.to_list |> List.first do
      %{"[applied]" => applied} -> if applied, do: {:ok, :success}, else: {:error, %{message: "Your operation was not applied."}}
      _ -> {:error, %{message: "Your operation was not applied."}}
    end
  end
  defp execute_error(error), do: error

  defp format_results(list) when is_list(list), do: list |> Enum.map(fn map -> string_to_atom_keys(map) end)
  defp format_results(_), do: nil

  defp string_to_atom_keys(list), do: list |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end) |> Enum.into(%{})
  defp atom_to_string_keys(list), do: list |> Enum.map(fn {k, v} -> {to_string(k), v} end) |> Enum.into(%{})

  defp cluster_for(query), do: Triton.Metadata.conn(query[:__schema_module__])
  defp dual_execute_cluster_for(query) do
    Triton.Metadata.secondary_conn(query[:__schema_module__])
  end
  defp dual_writes_enabled() do
    case Application.get_env(:triton, :enable_dual_writes) do
      true -> true
      "true" -> true
      _ -> false
    end
  end
  defp dual_reads_enabled() do
    case Application.get_env(:triton, :enable_dual_reads) do
      true -> true
      "true" -> true
      _ -> false
    end
  end

  defp auto_prepare(query) do
    cond do
      Triton.Configuration.enable_auto_prepare?()
      && is_nil(query[:prepared])
      && Triton.Helper.query_type(query) == :select
      ->
        Keyword.put(query, :prepared, :auto)

      true -> query
    end
  end
end
