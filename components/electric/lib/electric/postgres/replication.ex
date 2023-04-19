defmodule Electric.Postgres.Replication do
  use Electric.Satellite.Protobuf
  #
  # alias PgQuery, as: Pg
  # alias Electric.Postgres.{Dialect, Schema, Schema.AST, Schema.Proto}
  #
  # @type version() :: binary()
  #
  # @default_dialect Dialect.SQLite
  #
  # # we get a list of sql statements and a schema
  # # 1. update the schema with the ddl
  # # 2. generate the sqlite sql from the ast
  # # 3. get the list of tables involved in the migration
  # # 4. use the updated schema to get column, fk and pk information for the affected tables
  # #
  # # - creation of indexes doesn't affect any tables so that list should be empty
  # @spec migrate(Schema.t(), version(), [binary()], Electric.Postgres.Dialect.t()) ::
  #         {:ok, Schema.t(), Protocol.SatMigration.t()}
  # def migrate(schema, version, stmts, dialect \\ @default_dialect) do
  #   ast = Enum.flat_map(List.wrap(stmts), &Electric.Postgres.parse!/1)
  #   schema = Schema.update(schema, ast)
  #
  #   msg = build_replication_msg(ast, version, schema, dialect)
  #
  #   {:ok, schema, msg}
  # end
  #
  # def stmt_type(%Pg.CreateStmt{}) do
  #   :CREATE_TABLE
  # end
  #
  # def stmt_type(%Pg.IndexStmt{}) do
  #   :CREATE_INDEX
  # end
  #
  # def stmt_type(%Pg.AlterTableStmt{cmds: [cmd]}) do
  #   case cmd do
  #     %{node: {:alter_table_cmd, %Pg.AlterTableCmd{subtype: :AT_AddColumn}}} ->
  #       :ALTER_ADD_COLUMN
  #   end
  # end
  #
  # def affected_tables(stmts, dialect \\ @default_dialect) when is_list(stmts) do
  #   stmts
  #   |> Enum.flat_map(&get_affected_table/1)
  #   |> Enum.uniq_by(&Dialect.table_name(&1, dialect))
  # end
  #
  # defp get_affected_table(%Pg.CreateStmt{relation: relation}) do
  #   [AST.map(relation)]
  # end
  #
  # defp get_affected_table(%Pg.AlterTableStmt{relation: relation}) do
  #   [AST.map(relation)]
  # end
  #
  # defp get_affected_table(%Pg.IndexStmt{}) do
  #   []
  # end
  #
  # defp build_replication_msg(ast, version, schema, dialect) do
  #   tables =
  #     ast
  #     |> affected_tables(dialect)
  #     |> Enum.map(&Schema.fetch_table!(schema, &1))
  #     |> Enum.map(&replication_msg_table(&1, dialect))
  #
  #   stmts =
  #     Enum.map(
  #       ast,
  #       &%SatMigration.Stmt{
  #         type: stmt_type(&1),
  #         sql: Dialect.to_sql(&1, dialect)
  #       }
  #     )
  #
  #   %SatMigration{
  #     version: version,
  #     tables: tables,
  #     stmts: stmts
  #   }
  # end
  #
  # defp replication_msg_table(%Proto.Table{} = table, dialect) do
  #   %SatMigration.Table{
  #     name: Dialect.table_name(table.name, dialect),
  #     columns: Enum.map(table.columns, &replication_msg_table_col(&1, dialect)),
  #     fks: Enum.flat_map(table.constraints, &replication_msg_table_fk(&1, dialect)),
  #     pks: Enum.flat_map(table.constraints, &replication_msg_table_pk(&1, dialect))
  #   }
  # end
  #
  # defp replication_msg_table_col(%Proto.Column{} = column, dialect) do
  #   %SatMigration.Column{
  #     name: column.name,
  #     pg_type: replication_msg_table_col_type(column.type),
  #     sqlite_type: Dialect.type_name(column.type, dialect)
  #   }
  # end
  #
  # defp replication_msg_table_col_type(%Proto.Column.Type{} = type) do
  #   %SatMigration.PgColumnType{
  #     name: type.name,
  #     array: type.array,
  #     size: type.size
  #   }
  # end
  #
  # defp replication_msg_table_pk(%Proto.Constraint{constraint: {:primary, pk}}, _dialect) do
  #   pk.keys
  # end
  #
  # defp replication_msg_table_pk(_constraint, _dialect) do
  #   []
  # end
  #
  # defp replication_msg_table_fk(%Proto.Constraint{constraint: {:foreign, fk}}, dialect) do
  #   [
  #     %SatMigration.ForeignKey{
  #       fk_cols: fk.fk_cols,
  #       pk_cols: fk.pk_cols,
  #       pk_table: Dialect.table_name(fk.pk_table, dialect)
  #     }
  #   ]
  # end
  #
  # defp replication_msg_table_fk(_constraint, _dialect) do
  #   []
  # end
end
