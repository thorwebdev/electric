defmodule Electric.Postgres.ExtensionTest do
  use ExUnit.Case, async: false

  alias Electric.Postgres.{Extension, Schema}

  setup do
    pg_config = Electric.Postgres.TestConnection.config()

    {:ok, conn} = start_supervised(Electric.Postgres.TestConnection.childspec(pg_config))

    {:ok, conn: conn}
  end

  defmodule RollbackError do
    # use a special error to abort the transaction so we can be sure that some other problem isn't
    # happening in the tx and being swallowed
    defexception [:message]
  end

  def tx(fun, cxt) do
    assert_raise RollbackError, fn ->
      :epgsql.with_transaction(
        cxt.conn,
        fn tx ->
          fun.(tx)
          raise RollbackError, message: "rollback"
        end,
        reraise: true
      )
    end
  end

  def migrate(conn, cxt, migrations) do
    dir = cxt.tmp_dir

    versions =
      Enum.reduce(migrations, [], fn {version, name, sql}, acc ->
        filename = "#{version}_#{Macro.underscore(name)}.exs"
        filepath = Path.join(dir, filename)

        ex = """
        defmodule Electric.Postgres.ExtensionTest.#{name} do
          def up(schema) do
            ["#{sql}"]
          end
        end
        """

        File.write!(filepath, ex)
        [version | acc]
      end)
      |> Enum.reverse()

    {:ok, _} = Extension.migrate(conn, dir)

    assert {:ok, columns, rows} = :epgsql.squery(conn, "SELECT * FROM electric.schema_migrations")

    assert ["version", "inserted_at"] == Enum.map(columns, &elem(&1, 1))
    assert versions == Enum.map(rows, fn {v, _ts} -> String.to_integer(v) end)

    {:ok, _, rows} =
      :epgsql.equery(
        conn,
        "SELECT c.relname FROM pg_class c INNER JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = $1",
        ["electric"]
      )

    {:ok, Enum.map(rows, &Tuple.to_list(&1))}
  end

  @migrations [
    {2023_03_28_10_57_30, "CreateThing", "CREATE TABLE \#{schema}.things (id uuid PRIMARY KEY)"},
    {2023_03_28_10_57_31, "CreateOtherThing",
     "CREATE TABLE \#{schema}.other_things (id uuid PRIMARY KEY)"},
    {2023_03_28_10_57_32, "DropOtherThing", "DROP TABLE \#{schema}.other_things"}
  ]

  @tag :tmp_dir
  test "uses migration table to track applied migrations", cxt do
    tx(
      fn conn ->
        {:ok, rows} = migrate(conn, cxt, Enum.slice(@migrations, 0..0))
        assert rows == [["schema_migrations"], ["things"]]
        {:ok, rows} = migrate(conn, cxt, Enum.slice(@migrations, 0..1))
        assert rows == [["schema_migrations"], ["things"], ["other_things"]]
        {:ok, rows} = migrate(conn, cxt, Enum.slice(@migrations, 0..2))
        assert rows == [["schema_migrations"], ["things"]]
      end,
      cxt
    )
  end

  test "default migrations are valid", cxt do
    tx(
      fn conn ->
        {:ok, [2023_03_28_11_39_27]} = Extension.migrate(conn)
      end,
      cxt
    )
  end

  test "we can retrieve and set the current schema json", cxt do
    tx(
      fn conn ->
        {:ok, [2023_03_28_11_39_27]} = Extension.migrate(conn)

        assert {:ok, nil} = Extension.current_schema(conn)
        schema = Schema.new()
        version = "20230405171534_1"

        schema =
          Schema.update(
            schema,
            Electric.Postgres.parse!("CREATE TABLE first (id uuid PRIMARY KEY);")
          )

        assert :ok = Extension.save_schema(conn, version, schema)
        assert {:ok, ^version, ^schema} = Extension.current_schema(conn)

        schema =
          Schema.update(
            schema,
            Electric.Postgres.parse!("ALTER TABLE first ADD value text;")
          )

        version = "20230405171534_2"
        assert :ok = Extension.save_schema(conn, version, schema)
        assert {:ok, ^version, ^schema} = Extension.current_schema(conn)
      end,
      cxt
    )
  end

  test "migration capture", cxt do
    tx(
      fn conn ->
        {:ok, [2023_03_28_11_39_27]} = Extension.migrate(conn)

        sql1 = "CREATE TABLE buttercup (id int8 GENERATED ALWAYS AS IDENTITY);"
        sql2 = "CREATE TABLE daisy (id int8 GENERATED ALWAYS AS IDENTITY);"
        sql3 = "ALTER TABLE buttercup ADD COLUMN petal text;"
        sql4 = "ALTER TABLE buttercup ADD COLUMN stem text, ADD COLUMN leaf text;"

        for sql <- [sql1, sql2, sql3, sql4] do
          {:ok, _cols, _rows} = :epgsql.squery(conn, sql)
        end

        {:ok, [row1, row2, row3, row4]} = Extension.ddl_history(conn)

        assert {1, txid, timestamp, ^sql1} = row1
        assert {2, ^txid, ^timestamp, ^sql2} = row2
        assert {3, ^txid, ^timestamp, ^sql3} = row3
        assert {4, ^txid, ^timestamp, ^sql4} = row4
      end,
      cxt
    )
  end
end
