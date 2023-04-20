defmodule Electric.Postgres.Extension do
  @moduledoc """
  Manages our pseudo-extension code
  """

  alias Electric.Postgres.Schema.Proto

  @type conn() :: :epgsql.connection()
  @type version() :: pos_integer()
  @type versions() :: [version()]

  defmodule Error do
    defexception [:message]
  end

  @schema "electric"
  @migration_table ~s("#{@schema}"."schema_migrations")
  @ddl_table "ddl_commands"
  @schema_table "schema"

  @default_migration_path Path.expand("extension/migrations", __DIR__)
  @current_schema_query "SELECT schema, version FROM #{@schema}.#{@schema_table} ORDER BY id DESC LIMIT 1"
  @save_schema_query "INSERT INTO #{@schema}.#{@schema_table} (version, schema) VALUES ($1, $2)"
  @ddl_history_query "SELECT id, txid, txts, query FROM #{@schema}.#{@ddl_table} ORDER BY id ASC;"

  def schema, do: @schema
  def ddl_table, do: @ddl_table
  def schema_table, do: @schema_table

  def current_schema(conn) do
    with {:ok, [_, _], rows} <- :epgsql.equery(conn, @current_schema_query) do
      case rows do
        [] ->
          {:ok, nil}

        [{schema, version}] ->
          with {:ok, schema} <- Proto.Schema.json_decode(schema) do
            {:ok, version, schema}
          end
      end
    end
  end

  def save_schema(conn, version, %Proto.Schema{} = schema) do
    with {:ok, iodata} <- Proto.Schema.json_encode(schema),
         json = IO.iodata_to_binary(iodata),
         {:ok, 1} <- :epgsql.equery(conn, @save_schema_query, [version, json]) do
      :ok
    end
  end

  @spec migrate(conn(), Path.t()) :: {:ok, versions()} | {:error, term()}
  def migrate(conn, path \\ @default_migration_path) do
    migrations = migrations(path)

    if Enum.empty?(migrations), do: raise(Error, message: "no migration files in #{path}")

    ensure_transaction(conn, fn txconn ->
      create_schema(txconn)
      create_migration_table(txconn)

      with_migration_lock(txconn, fn ->
        existing_migrations = existing_migrations(txconn)

        versions =
          migrations
          |> Enum.reject(fn {version, _, _path} -> version in existing_migrations end)
          |> Enum.flat_map(fn {version, name, path} ->
            Enum.map(compile_file(path), &{version, name, path, &1})
          end)
          |> Enum.reduce([], fn {version, _name, _path, module}, v ->
            for sql <- module.up(@schema) do
              {:ok, _cols, _rows} = :epgsql.squery(txconn, sql)
            end

            {:ok, _count} =
              :epgsql.squery(
                txconn,
                "INSERT INTO #{@migration_table} (version) VALUES ('#{version}')"
              )

            [version | v]
          end)
          |> Enum.reverse()

        {:ok, versions}
      end)
    end)
  end

  defp compile_file(path) do
    path
    |> Code.compile_file()
    |> Enum.map(&elem(&1, 0))
  end

  # https://dba.stackexchange.com/a/311714
  @is_transaction_sql "SELECT transaction_timestamp() != statement_timestamp() AS is_transaction"

  defp ensure_transaction(conn, fun) when is_function(fun, 1) do
    case :epgsql.squery(conn, @is_transaction_sql) do
      {:ok, _cols, [{"t"}]} ->
        fun.(conn)

      {:ok, _cols, [{"f"}]} ->
        :epgsql.with_transaction(conn, fun)
    end
  end

  def create_schema(conn) do
    ddl(conn, "CREATE SCHEMA IF NOT EXISTS #{@schema}")
  end

  @create_migration_table_sql """
  CREATE TABLE IF NOT EXISTS #{@migration_table} (
    version int8 NOT NULL PRIMARY KEY,
    inserted_at timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP
  );
  """

  def create_migration_table(conn) do
    ddl(conn, @create_migration_table_sql)
  end

  defp with_migration_lock(conn, fun) do
    ddl(conn, "LOCK TABLE #{@migration_table} IN SHARE UPDATE EXCLUSIVE MODE")
    fun.()
  end

  defp existing_migrations(conn) do
    {:ok, _cols, rows} =
      :epgsql.squery(conn, "SELECT version FROM #{@migration_table} ORDER BY version ASC")

    Enum.map(rows, fn {version} -> String.to_integer(version) end)
  end

  defp ddl(conn, sql, _bind \\ []) do
    case :epgsql.squery(conn, sql) do
      {:ok, _count} -> conn
      {:ok, _count, _cols, _rows} -> conn
      {:ok, _cols, _rows} -> conn
    end
  end

  def migrations(dir) when is_binary(dir) do
    dir
    |> migration_files()
    |> split_paths()
  end

  def migration_files(dir) when is_binary(dir) do
    Path.wildcard(Path.join(dir, "*.exs"))
  end

  defp split_paths(paths) when is_list(paths) do
    paths
    |> Enum.map(&extract_migration_info/1)
    |> Enum.filter(& &1)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp extract_migration_info(file) do
    base = Path.basename(file)

    case Integer.parse(Path.rootname(base)) do
      {integer, "_" <> name} -> {integer, name, file}
      _ -> nil
    end
  end

  def ddl_history(conn) do
    with {:ok, _cols, rows} <- :epgsql.equery(conn, @ddl_history_query, []) do
      {:ok, rows}
    end
  end
end
