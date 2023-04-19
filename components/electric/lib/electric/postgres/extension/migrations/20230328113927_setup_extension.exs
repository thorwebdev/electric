defmodule Electric.Postgres.Extension.Migrations.SetupExtension do
  alias Electric.Postgres.Extension

  def up(schema) do
    ddl_table = "#{schema}.#{Extension.ddl_table()}"
    schema_table = "#{schema}.#{Extension.schema_table()}"

    [
      """
      CREATE TABLE #{ddl_table} (
         id int8 NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
         txid int8 NOT NULL,
         txts timestamp with time zone NOT NULL,
         query text NOT NULL
      );
      """,
      ##################
      """
      CREATE TABLE #{schema_table} (
         id int8 NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
         version varchar(255) NOT NULL,
         schema jsonb NOT NULL,
         created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      """,
      ##################
      """
      CREATE OR REPLACE FUNCTION #{schema}.create_active_migration() RETURNS int8 AS
      $function$
      DECLARE
          trid int8;
      BEGIN
          INSERT INTO #{ddl_table} (txid, txts, query) VALUES (txid_current(), transaction_timestamp(), current_query()) RETURNING id INTO trid;
          RETURN trid;
      END;
      $function$
      LANGUAGE PLPGSQL;
      """,
      ##################
      """
      CREATE OR REPLACE FUNCTION #{schema}.active_migration_id() RETURNS int8 AS
      $function$
      DECLARE
          trid int8;
      BEGIN
          SELECT id INTO trid FROM #{ddl_table} WHERE txid = txid_current() AND txts = transaction_timestamp() ORDER BY id DESC LIMIT 1;
          RETURN trid;
      END;
      $function$
      LANGUAGE PLPGSQL;
      """,
      ##################
      """
      CREATE OR REPLACE FUNCTION #{schema}.complete_migration(trid int8) RETURNS int8 AS
      $function$
      BEGIN
          UPDATE #{ddl_table} SET completed = true WHERE id = trid;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'no transaction with id %s found', trid;
          END IF;
          RETURN trid;
      END;
      $function$
      LANGUAGE PLPGSQL;
      """,
      ##################
      """
      CREATE OR REPLACE FUNCTION #{schema}.ddlx_command_start_handler() RETURNS EVENT_TRIGGER AS
      $function$
      DECLARE
          trid int8;
      BEGIN
          RAISE INFO 'command_start_handler: start';
          trid := (SELECT #{schema}.create_active_migration());
          RAISE INFO 'command_start_handler: %', trid;
          RAISE INFO 'command_start_handler: end %', trid;
      END;
      $function$
      LANGUAGE PLPGSQL;
      """,
      ##################
      """
      CREATE OR REPLACE FUNCTION #{schema}.ddlx_command_end_handler() RETURNS EVENT_TRIGGER AS
      $function$
      DECLARE
          trid int8;
          -- v_cmd_rec record;
          -- do_insert_cmd boolean;
      BEGIN
          trid := (SELECT #{schema}.active_migration_id());
          RAISE INFO 'command_end_handler: start %', trid;
          -- FOR v_cmd_rec IN SELECT * FROM pg_event_trigger_ddl_commands()
          -- LOOP
          --   do_insert_cmd := true;
          --   RAISE INFO 'command type %', v_cmd_rec.command_tag;

          --   IF v_cmd_rec.command_tag = 'CREATE TABLE' THEN
          --     RAISE INFO 'CREATE TABLE...';
          --   ELSIF v_cmd_rec.command_tag = 'CREATE INDEX' THEN
          --     RAISE INFO 'CREATE INDEX...';
          --   ELSIF v_cmd_rec.command_tag = 'ALTER TABLE' THEN
          --     IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() WHERE objid = v_cmd_rec.objid AND command_tag = 'CREATE TABLE') THEN
          --       -- the table being altered is also being created in the same transaction. so we can just ignore this
          --       RAISE INFO 'CREATE + ALTER TABLE... %', v_cmd_rec.object_type;
          --       do_insert_cmd := false;
          --     ELSE
          --       RAISE INFO 'ALTER TABLE... %', v_cmd_rec.object_type;
          --     END IF;
          --   END IF;
          --   IF do_insert_cmd THEN
          --       INSERT INTO #{"@ddl_tbl"} (trid, trig, classid, objid, objsubid, command_tag, object_type, schema_name, object_identity)
          --       VALUES (
          --           trid,
          --           'e',
          --           v_cmd_rec.classid,
          --           v_cmd_rec.objid,
          --           v_cmd_rec.objsubid,
          --           v_cmd_rec.command_tag,
          --           v_cmd_rec.object_type,
          --           v_cmd_rec.schema_name,
          --           -- ARRAY[v_cmd_rec.object_identity]
          --           parse_ident(v_cmd_rec.object_identity)
          --       );
          --   END IF;

          -- END LOOP;
          -- PERFORM #{schema}.complete_migration(trid);
          RAISE INFO 'command_end_handler: end %', trid;
      END;
      $function$
      LANGUAGE PLPGSQL;
      """,
      ##################
      """
      CREATE EVENT TRIGGER #{schema}_event_trigger_ddl_start ON ddl_command_start
          EXECUTE FUNCTION #{schema}.ddlx_command_start_handler();
      """,
      ##################
      """
      CREATE EVENT TRIGGER #{schema}_event_trigger_ddl_end ON ddl_command_end
          EXECUTE FUNCTION #{schema}.ddlx_command_end_handler();

      """
    ]
  end
end
