defmodule Triton.Executor do
  defmacro __using__(_) do
    quote do
      def all(query, options \\ []) do
        case Triton.Executor.execute(query, options) do
          {:error, err} -> {:error, err.message}
          results -> results
        end
      end

      @doc """
      Use this option if you are trying to a lot of data...
      """
      def streamed(query, options \\ []) do
        case Triton.Executor.execute([{:streamed, true} | query], options) do
          {:error, err} -> {:error, err.message}
          results -> results
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

      def delete(query) do
        case Triton.Executor.execute(query) do
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

    end
  end

  @doc """
  Batch execute, like execute, but on a list of queries
  """
  def batch_execute(queries, options \\ []) do
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

  @doc """
  Creates a valid CQL query out of a query keyword list and executes it.
  Returns {:ok, results}
          {:error, error}
  """
  def execute(query, options \\ []) do
    with {:ok, query}     <- Triton.Validate.coerce(query),
         {:ok, type, cql} <- build_cql(query),
         {:ok, results}   <- execute_cql(type, cql, query[:prepared], options),
    do: {:ok, results}
  end

  defp build_cql(query) do
    case Triton.Helper.query_type(query) do
      :streamed -> {:ok, :streamed, Triton.CQL.Select.build(query)}
      :count  -> {:ok, :count, Triton.CQL.Select.build(query)}
      :select -> {:ok, :select, Triton.CQL.Select.build(query)}
      :insert -> {:ok, :insert, Triton.CQL.Insert.build(query)}
      :update -> {:ok, :update, Triton.CQL.Update.build(query)}
      :delete -> {:ok, :delete, Triton.CQL.Delete.build(query)}
      err -> err
    end
  end

  defp execute_cql(_, _, _, options \\ [])

  defp execute_cql(:streamed, cql, nil, options) do
    with pages <- Xandra.stream_pages!(Triton.Conn, cql, [], [pool: Xandra.Cluster] ++ options) do
      results = pages
        |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
      {:ok, results}
    end
  end
  defp execute_cql(:streamed, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(Triton.Conn, cql, [pool: Xandra.Cluster] ++ options),
         pages <- Xandra.stream_pages!(Triton.Conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options)
    do
      results = pages
        |> Stream.flat_map(fn page -> Enum.to_list(page) |> format_results end)
      {:ok, results}
    end
  end

  defp execute_cql(:select, cql, nil, options) do
    with {:ok, page} <- Xandra.execute(Triton.Conn, cql, [], [pool: Xandra.Cluster] ++ options),
      do: {:ok, Enum.to_list(page) |> format_results}
  end
  defp execute_cql(:select, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(Triton.Conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, page} <- Xandra.execute(Triton.Conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options),
      do: {:ok, Enum.to_list(page) |> format_results}
  end

  defp execute_cql(:count, cql, nil, options) do
    with {:ok, page} <- Xandra.execute(Triton.Conn, cql, [], [pool: Xandra.Cluster] ++ options),
         count <- page |> Enum.to_list |> List.first |> Map.get("count"),
      do: {:ok, count}
  end
  defp execute_cql(:count, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(Triton.Conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, page} <- Xandra.execute(Triton.Conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options),
         count <- page |> Enum.to_list |> List.first |> Map.get("count"),
      do: {:ok, count}
  end

  defp execute_cql(_, cql, nil, options) do
    with {:ok, %Xandra.Void{}} <- Xandra.execute(Triton.Conn, cql, [], [pool: Xandra.Cluster] ++ options) do
      {:ok, :success}
    else
      error -> error |> execute_error
    end
  end
  defp execute_cql(_, cql, prepared, options) do
    with {:ok, statement} <- Xandra.prepare(Triton.Conn, cql, [pool: Xandra.Cluster] ++ options),
         {:ok, %Xandra.Void{}} <- Xandra.execute(Triton.Conn, statement, atom_to_string_keys(prepared), [pool: Xandra.Cluster] ++ options)
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
end
