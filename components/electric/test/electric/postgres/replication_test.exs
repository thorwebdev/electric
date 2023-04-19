defmodule Electric.Postgres.ReplicationTest do
  use ExUnit.Case, async: true
  #
  use Electric.Satellite.Protobuf
  #
  #   alias Electric.Postgres.{Replication, Schema}
  #
  #   def parse(sql) do
  #     Electric.Postgres.parse!(sql)
  #   end
  #
  #   test "stmt_type/1" do
  #     stmts = [
  #       {"create table doorbel (id int8);", :CREATE_TABLE},
  #       {"create index on frog (id asc);", :CREATE_INDEX},
  #       {"alter table public.fish add value text;", :ALTER_ADD_COLUMN}
  #     ]
  #
  #     for {sql, expected_type} <- stmts do
  #       [ast] = parse(sql)
  #       assert Replication.stmt_type(ast) == expected_type
  #     end
  #   end
  #
  #   describe "affected_tables/1" do
  #     def assert_table_list(tables, expected_tables) do
  #       assert length(tables) == length(expected_tables)
  #
  #       assert tables
  #              |> Enum.zip(expected_tables)
  #              |> Enum.all?(fn {name, expected_name} ->
  #                Schema.equal?(name, expected_name, [nil])
  #              end)
  #     end
  #
  #     test "returns a list of created tables" do
  #       """
  #       create table public.fish (id int8 primary key);
  #       create table frog (id int8 primary key);
  #       create table teeth.front (id int8 primary key);
  #       """
  #       |> parse()
  #       |> Replication.affected_tables()
  #       |> assert_table_list([{"public", "fish"}, {nil, "frog"}, {"teeth", "front"}])
  #     end
  #
  #     test "returns a list of altered tables" do
  #       """
  #       alter table public.fish add value text;
  #       alter table frog add constraint "something_unique" unique (something);
  #       alter table teeth.front alter column id drop default;
  #       """
  #       |> parse()
  #       |> Replication.affected_tables()
  #       |> assert_table_list([{"public", "fish"}, {nil, "frog"}, {"teeth", "front"}])
  #     end
  #
  #     test "captures all affected tables" do
  #       """
  #       create table public.fish (id int8 primary key);
  #       create table frog (id int8 primary key);
  #       alter table teeth.front alter column id drop default;
  #       """
  #       |> parse()
  #       |> Replication.affected_tables()
  #       |> assert_table_list([{"public", "fish"}, {nil, "frog"}, {"teeth", "front"}])
  #     end
  #
  #     test "deduplicates in a search path aware manner" do
  #       """
  #       create table public.fish (id int8 primary key);
  #       alter table fish alter column id drop default;
  #       """
  #       |> parse()
  #       |> Replication.affected_tables()
  #       |> assert_table_list([{"public", "fish"}])
  #     end
  #
  #     test "returns [] for CREATE INDEX" do
  #       """
  #       create index my_index on public.fish (id);
  #       create index on frog (id asc);
  #       """
  #       |> parse()
  #       |> Replication.affected_tables()
  #       |> assert_table_list([])
  #     end
  #   end
  #
  #   describe "migrate/2" do
  #     test "updates the schema and returns a valid protcol message" do
  #       schema = Schema.new()
  #
  #       stmts = [
  #         """
  #         CREATE TABLE public.fish (id int8 PRIMARY KEY);
  #         CREATE TABLE frog (id int8 PRIMARY KEY);
  #         CREATE TABLE teeth.front (
  #             id int8 PRIMARY KEY,
  #             frog_id int8 NOT NULL REFERENCES fish (id)
  #         );
  #         """
  #       ]
  #
  #       version = "20230405134615"
  #
  #       assert {:ok, schema, msg} = Replication.migrate(schema, version, stmts)
  #
  #       # there are lots of tests that validate the schema is being properly updated
  #       assert Schema.table_names(schema) == [~s("public"."fish"), ~s("frog"), ~s("teeth"."front")]
  #       assert %SatMigration{version: ^version} = msg
  #       %{stmts: stmts, tables: tables} = msg
  #
  #       assert stmts == [
  #                %SatMigration.Stmt{
  #                  type: :CREATE_TABLE,
  #                  sql:
  #                    "CREATE TABLE \"fish\" (\n  \"id\" INTEGER NOT NULL,\n  CONSTRAINT \"fish_pkey\" PRIMARY KEY (\"id\")\n) WITHOUT ROWID;\n"
  #                },
  #                %SatMigration.Stmt{
  #                  type: :CREATE_TABLE,
  #                  sql:
  #                    "CREATE TABLE \"frog\" (\n  \"id\" INTEGER NOT NULL,\n  CONSTRAINT \"frog_pkey\" PRIMARY KEY (\"id\")\n) WITHOUT ROWID;\n"
  #                },
  #                %SatMigration.Stmt{
  #                  type: :CREATE_TABLE,
  #                  sql:
  #                    "CREATE TABLE \"front\" (\n  \"id\" INTEGER NOT NULL,\n  \"frog_id\" INTEGER NOT NULL,\n  CONSTRAINT \"front_frog_id_fkey\" FOREIGN KEY (\"frog_id\") REFERENCES \"fish\" (\"id\"),\n  CONSTRAINT \"front_pkey\" PRIMARY KEY (\"id\")\n) WITHOUT ROWID;\n"
  #                }
  #              ]
  #
  #       assert [
  #                %SatMigration.Table{name: "fish"} = table1,
  #                %SatMigration.Table{name: "frog"} = table2,
  #                %SatMigration.Table{name: "front"} = table3
  #              ] = tables
  #
  #       assert table1 == %SatMigration.Table{
  #                name: "fish",
  #                columns: [
  #                  %SatMigration.Column{
  #                    name: "id",
  #                    sqlite_type: "INTEGER",
  #                    pg_type: %SatMigration.PgColumnType{name: "int8", array: [], size: []}
  #                  }
  #                ],
  #                fks: [],
  #                pks: ["id"]
  #              }
  #
  #       assert table2 == %SatMigration.Table{
  #                name: "frog",
  #                columns: [
  #                  %SatMigration.Column{
  #                    name: "id",
  #                    sqlite_type: "INTEGER",
  #                    pg_type: %SatMigration.PgColumnType{name: "int8", array: [], size: []}
  #                  }
  #                ],
  #                fks: [],
  #                pks: ["id"]
  #              }
  #
  #       assert table3 == %SatMigration.Table{
  #                name: "front",
  #                columns: [
  #                  %SatMigration.Column{
  #                    name: "id",
  #                    sqlite_type: "INTEGER",
  #                    pg_type: %SatMigration.PgColumnType{
  #                      name: "int8",
  #                      array: [],
  #                      size: []
  #                    }
  #                  },
  #                  %SatMigration.Column{
  #                    name: "frog_id",
  #                    sqlite_type: "INTEGER",
  #                    pg_type: %SatMigration.PgColumnType{name: "int8", array: [], size: []}
  #                  }
  #                ],
  #                fks: [
  #                  %SatMigration.ForeignKey{
  #                    fk_cols: ["frog_id"],
  #                    pk_table: "fish",
  #                    pk_cols: ["id"]
  #                  }
  #                ],
  #                pks: ["id"]
  #              }
  #     end
  #
  #     test "multiple alter table cmds" do
  #       schema = Schema.new()
  #
  #       stmts = [
  #         """
  #         CREATE TABLE public.fish (id int8 PRIMARY KEY);
  #         """
  #       ]
  #
  #       version = "20230405134615"
  #
  #       assert {:ok, schema, _msg} = Replication.migrate(schema, version, stmts)
  #
  #       # there are lots of tests that validate the schema is being properly updated
  #       assert Schema.table_names(schema) == [~s("public"."fish")]
  #
  #       stmts = [
  #         """
  #         ALTER TABLE fish ADD COLUMN value jsonb DEFAULT '{}', ADD COLUMN ts timestamp DEFAULT current_timestamp;
  #         """
  #       ]
  #
  #       assert {:ok, _schema, msg} = Replication.migrate(schema, version, stmts)
  #
  #       assert %SatMigration{version: ^version} = msg
  #
  #       %{stmts: stmts, tables: tables} = msg
  #
  #       assert stmts == [
  #                %Electric.Satellite.V11.SatMigration.Stmt{
  #                  type: :ALTER_ADD_COLUMN,
  #                  sql: "ALTER TABLE \"fish\" ADD COLUMN \"value\" TEXT_JSON DEFAULT '{}';\n"
  #                },
  #                %Electric.Satellite.V11.SatMigration.Stmt{
  #                  type: :ALTER_ADD_COLUMN,
  #                  sql: "ALTER TABLE \"fish\" ADD COLUMN \"ts\" TEXT DEFAULT current_timestamp;\n"
  #                }
  #              ]
  #
  #       assert [table] = tables
  #
  #       assert table == %SatMigration.Table{
  #                name: "fish",
  #                columns: [
  #                  %SatMigration.Column{
  #                    name: "id",
  #                    sqlite_type: "INTEGER",
  #                    pg_type: %SatMigration.PgColumnType{name: "int8"}
  #                  },
  #                  %SatMigration.Column{
  #                    name: "value",
  #                    sqlite_type: "TEXT_JSON",
  #                    pg_type: %SatMigration.PgColumnType{name: "jsonb"}
  #                  },
  #                  %SatMigration.Column{
  #                    name: "ts",
  #                    sqlite_type: "TEXT",
  #                    pg_type: %SatMigration.PgColumnType{name: "timestamp"}
  #                  }
  #                ],
  #                fks: [],
  #                pks: ["id"]
  #              }
  #     end
  #
  #     test "create index doesn't list tables" do
  #       schema = Schema.new()
  #
  #       stmts = [
  #         """
  #         CREATE TABLE public.fish (id int8 PRIMARY KEY, available boolean);
  #         """
  #       ]
  #
  #       version = "20230405134615"
  #
  #       assert {:ok, schema, _msg} = Replication.migrate(schema, version, stmts)
  #
  #       stmts = [
  #         """
  #         CREATE INDEX fish_available_index ON public.fish (avilable);
  #         """
  #       ]
  #
  #       version = "20230405134616"
  #       assert {:ok, _schema, msg} = Replication.migrate(schema, version, stmts)
  #       assert %SatMigration{version: ^version} = msg
  #
  #       %{stmts: stmts, tables: tables} = msg
  #
  #       assert stmts == [
  #                %SatMigration.Stmt{
  #                  sql: "CREATE INDEX \"fish_available_index\" ON \"fish\" (\"avilable\" ASC);\n",
  #                  type: :CREATE_INDEX
  #                }
  #              ]
  #
  #       assert [] = tables
  #     end
  #
  #     # TODO: actually I think this is a situation we *MUST* avoid by
  #     # checking for unsupported migrations in the pg event trigger
  #     # function. by the time it reaches this point it would be too late
  #     # and things would be completely fubar'd
  #     # see: VAX-618
  #     # test "rejects unsupported migration types" do
  #     #   schema = Schema.new()
  #
  #     #   stmts = [
  #     #     """
  #     #     CREATE TABLE public.fish (id int8 PRIMARY KEY, value varchar(255));
  #     #     ALTER TABLE fish DROP COLUMN value;
  #     #     """
  #     #   ]
  #
  #     #   version = "20230405134615"
  #
  #     #   assert {:error, schema} = Replication.migrate(schema, version, stmts)
  #     # end
  #   end
end
