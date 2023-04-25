defmodule Electric.Postgres.Extension.Migrations.Migration_20230424154425_DDLX do
  alias Electric.Postgres.Extension

  @behaviour Extension.Migration
  sql_file = Path.expand("20230424154425_ddlx/ddlx.sql", __DIR__)
  @external_resource sql_file
  @sql File.read!(sql_file)

  @impl true
  def version, do: 2023_04_24_15_44_25

  @impl true
  def up(schema) do
    [
      String.replace(@sql, "#schema#", schema)
    ]
  end

  @impl true
  def down(schema) do
    []
  end
end
