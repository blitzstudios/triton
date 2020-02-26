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
        for result <- results do
          for {k, v} <- result, into: %{} do
            case query[:__schema__].__fields__[k][:opts][:transform] do
              nil -> {k, v}
              func -> {k, func.(v)}
            end
          end
        end
      end
      defp transform_results(_, results), do: results
    end
  end

  @doc """
  Batch execute, like execute, but on a list of queries
  """
  def batch_execute(queries, options \\ [])
  def batch_execute(queries, options) when is_list(queries) and length(queries) > 0 do
    cluster = List.first(queries) |> cluster_for
    dual_write_cluster =
      case dual_writes_enabled() do
        true -> List.first(queries) |> dual_write_cluster_for
        _ -> nil
      end
    cqls =
      queries
      |> Enum.map(fn query ->
        {:ok, _, cql} = build_cql(query)
        {cql, query[:prepared]}
      end)

    execute =
      fn cluster ->
        Xandra.Cluster.run(cluster, fn conn ->
          batch =
            cqls
            |> Enum.reduce(Xandra.Batch.new(), fn ({cql, prepared}, acc) ->
              case prepared do
                nil -> Xandra.Batch.add(acc, cql)
                prepared -> Xandra.Batch.add(acc, Xandra.prepare(conn, cql, options), prepared)
              end
            end)

          with {:ok, %Xandra.Void{}} <- Xandra.execute(conn, batch, options),
               do: {:ok, :success}
        end)
      end

    cluster_result = execute.(cluster)

    _ = try do
      case dual_write_cluster do
        nil -> :noop
        _ -> execute.(dual_write_cluster)
      end
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

    cluster_result
  end
  def batch_execute(_, _), do: {:ok, :success}

  @doc """
  Creates a valid CQL query out of a query keyword list and executes it.
  Returns {:ok, results}
          {:error, error}
  """
  def execute(query, options \\ []) do
    with cluster <- cluster_for(query),
      dual_write_cluster <- dual_write_cluster_for(query),
      type <- Triton.Helper.query_type(query),
      {:ok, results} <-
        (result = execute_on_cluster(query, cluster, options)
         _ = try do
           cond do
             dual_writes_enabled() && (not is_nil(dual_write_cluster)) && type in [:insert, :update, :delete] ->
               execute_on_cluster(query, dual_write_cluster, options)
             true -> :noop
           end
         rescue
           err -> {:error, err}
         catch
           ex -> {:error, ex}
           :exit, ex -> {:error, ex}
         end
         |> case do
              {:error, err} -> Logger.error("Triton execute dual write error: #{inspect(err)}, query: #{inspect(query)}")
              _ -> :noop
            end

         result)
    do
      {:ok, results}
    end
  end

  def execute_on_cluster(query, cluster, options \\ []) do
    with {:ok, query}     <- Triton.Validate.coerce(query),
         {:ok, type, cql} <- build_cql(query),
         {:ok, results}   <- execute_cql(cluster, type, cql, query[:prepared], options)
    do
      {:ok, results}
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
      do: {:ok, Enum.to_list(page) |> format_results}
  end
  defp execute_cql(cluster, :select, cql, prepared, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      with {:ok, statement} <- Xandra.prepare(conn, cql, options),
           {:ok, page} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), options),
        do: {:ok, Enum.to_list(page) |> format_results}
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

  defp cluster_for(query), do: query[:__schema__].__keyspace__.__struct__.__conn__
  defp dual_write_cluster_for(query) do
    case query[:__schema__].__dual_write_keyspace__ do
      nil -> nil
      keyspace -> keyspace.__struct__.__conn__
    end
  end
  defp dual_writes_enabled() do
    case Application.get_env(:triton, :enable_dual_writes) do
      true -> true
      "true" -> true
      _ -> false
    end
  end
end
