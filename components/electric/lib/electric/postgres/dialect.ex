defmodule Electric.Postgres.Dialect do
  alias PgQuery, as: Pg
  alias Electric.Postgres.Schema.Proto

  @type sql() :: binary()
  @type t() :: module()
  @type name() :: Pg.RangeVar.t() | Proto.RangeVar.t()
  @type base_type() :: binary()

  @callback table_name(name()) :: binary() | no_return()
  @callback to_sql(Pg.t(), Keyword.t()) :: sql() | no_return()
  @callback type_name(Proto.Column.Type.t()) :: sql() | no_return()

  @spec to_sql(Pg.t(), t(), Keyword.t()) :: {:ok, sql()} | {:error, term}
  def to_sql(model, dialect, opts \\ []) do
    dialect.to_sql(model, opts)
  end

  @spec table_name(name(), t(), Keyword.t()) :: sql | no_return
  def table_name(name, dialect, opts \\ []) do
    dialect.table_name(name, opts)
  end

  @spec type_name(Proto.Column.Type.t(), t(), Keyword.t()) :: sql | no_return
  def type_name(type, dialect, opts \\ []) do
    dialect.type_name(type, opts)
  end
end
