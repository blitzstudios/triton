defmodule Triton.Query do
  alias Triton.QueryBuilder

  @limit_default 100

  defmacro prepared(module, prepared \\ :auto) do
    QueryBuilder.build_query(:prepared, module, prepared)
  end

  defmacro select(module, select \\ []) do
    QueryBuilder.build_query(:select, module, select)
  end

  defmacro where(module, where \\ []) do
    QueryBuilder.build_query(:where, module, where)
  end

  defmacro limit(module, limit \\ @limit_default) do
    QueryBuilder.build_query(:limit, module, limit)
  end

  defmacro order_by(module, order_by \\ []) do
    QueryBuilder.build_query(:order_by, module, order_by)
  end

  defmacro allow_filtering(module) do
    QueryBuilder.build_query(:allow_filtering, module, true)
  end

  defmacro insert(module, insert \\ []) do
    QueryBuilder.build_query(:insert, module, insert)
  end

  defmacro if_not_exists(module) do
    QueryBuilder.build_query(:if_not_exists, module, true)
  end

  defmacro if_exists(module) do
    QueryBuilder.build_query(:if_exists, module, true)
  end

  defmacro update(module, update \\ []) do
    QueryBuilder.build_query(:update, module, update)
  end

  defmacro constrain(module, constrain \\ []) do
    QueryBuilder.build_query(:constrain, module, constrain)
  end

  defmacro delete(module, delete \\ []) do
    QueryBuilder.build_query(:delete, module, delete)
  end
end
