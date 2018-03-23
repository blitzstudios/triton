defmodule Triton.Executor do
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
    conn = List.first(queries) |> conn_for
    batch = queries
      |> Enum.map(fn query ->
        {:ok, _, cql} = build_cql(query)
        {cql, query[:prepared]}
      end)
      |> Enum.reduce(Xandra.Batch.new(), fn ({cql, prepared}, acc) ->
        case prepared do
          nil -> Xandra.Batch.add(acc, cql)
          prepared -> Xandra.Batch.add(acc, Xandra.prepare(Triton.Conn, cql, [pool: Xandra.Cluster] ++ options), prepared)
        end
      end)

    with {:ok, %Xandra.Void{}} <- Xandra.execute(Triton.Conn, batch, [pool: Xandra.Cluster] ++ options),
      do: {:ok, :success}
  end
  def batch_execute(_, _), do: {:ok, :success}

  @doc """
  Creates a valid CQL query out of a query keyword list and executes it.
  Returns {:ok, results}
          {:error, error}
  """
  def execute(query, options \\ []) do
    with {:ok, query}     <- Triton.Validate.coerce(query),
         {:ok, type, cql} <- build_cql(query),
         {:ok, results}   <- execute_cql(conn_for(query), type, cql, query[:prepared], options),
    do: {:ok, results}
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

  defp execute_cql(conn, :stream, cql, nil, options) do
    with pages <- Xandra.stream_pages!(conn, cql, [], [pool: Xandra.Cluster] ++ options) do
      results = pages
        |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
      {:ok, results}
    end
  end
  defp execute_cql(conn, :stream, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(conn, cql, [pool: Xandra.Cluster] ++ options),
         pages <- Xandra.stream_pages!(conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options)
    do
      results = pages
        |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
      {:ok, results}
    end
  end

  defp execute_cql(conn, :select, cql, nil, options) do
    with {:ok, page} <- Xandra.execute(conn, cql, [], [pool: Xandra.Cluster] ++ options),
      do: {:ok, Enum.to_list(page) |> format_results}
  end
  defp execute_cql(conn, :select, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, page} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options),
      do: {:ok, Enum.to_list(page) |> format_results}
  end

  defp execute_cql(conn, :count, cql, nil, options) do
    with {:ok, page} <- Xandra.execute(conn, cql, [], [pool: Xandra.Cluster] ++ options),
         count <- page |> Enum.to_list |> List.first |> Map.get("count"),
      do: {:ok, count}
  end
  defp execute_cql(conn, :count, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, page} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options),
         count <- page |> Enum.to_list |> List.first |> Map.get("count"),
      do: {:ok, count}
  end

  defp execute_cql(conn, _, cql, nil, options) do
    with {:ok, %Xandra.Void{}} <- Xandra.execute(conn, cql, [], [pool: Xandra.Cluster] ++ options) do
      {:ok, :success}
    else
      error -> error |> execute_error
    end
  end
  defp execute_cql(conn, _, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, %Xandra.Void{}} <- Xandra.execute(conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options)
    do
      {:ok, :success}
    else
      error -> error |> execute_error
    end
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

  defp conn_for(query), do: query[:__schema__].__keyspace__.__struct__.__conn__
end
