


        -- create a stored procedure for logs
        CREATE OR REPLACE PROCEDURE log.log_script(var_dag_name VARCHAR, var_script_name VARCHAR, var_stage_type VARCHAR)
        LANGUAGE 'plpgsql'
        AS $log_script$
        DECLARE
            var_stage_time TIMESTAMP;
            var_main_table VARCHAR(100);
            var_table_row_count BIGINT;
        BEGIN
            EXECUTE 'SELECT CLOCK_TIMESTAMP()' INTO var_stage_time;
            INSERT INTO log.scripts_logs(
                dag_name,
                script_name,
                stage_type,
                stage_time,
                main_table,
                table_row_count
            )
            VALUES (
                var_dag_name,
                var_script_name,
                var_stage_type,
                var_stage_time,
                var_main_table,
                var_table_row_count
            );
        END;
        $log_script$;

        -- Create a trigger function to log the start or the end of a process
        CREATE OR REPLACE FUNCTION log.log_process()
        RETURNS TRIGGER
        LANGUAGE 'plpgsql'
        AS $log_process$
        DECLARE
            table_row_count BIGINT;
            stage_time TIMESTAMP;
        BEGIN
            EXECUTE 'SELECT COUNT(1) FROM   ' || TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME  INTO table_row_count;
            EXECUTE 'SELECT CLOCK_TIMESTAMP() ' INTO stage_time;
            INSERT INTO log.processes_logs(
                process_id,
                stage_type,
                name_schema,
                name_table,
                process_type,
                table_row_count,
                stage_time
            ) VALUES (
                TG_RELID,
                TG_WHEN,
                TG_TABLE_SCHEMA,
                TG_TABLE_NAME,
                TG_OP,
                table_row_count,
                stage_time
            );
            RETURN NULL;
        END;
        $log_process$;

        -- Create logging triggers for all existing tables, except for log.processes_logs
        DO
        $$
        DECLARE
            table_infos record;
        BEGIN
            FOR table_infos IN (
                SELECT schemaname, tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname in  ( 'public' , 'raw' , 'source' )
                    AND tablename <> 'processes_logs'
            )

            LOOP
            -- Logging trigger to notify the start of a process
            EXECUTE FORMAT('DROP TRIGGER IF EXISTS log_%s_%s_start ON %s.%s;',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            EXECUTE FORMAT('CREATE TRIGGER log_%s_%s_start BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s.%s FOR EACH STATEMENT EXECUTE FUNCTION log.log_process()',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);

            -- Logging trigger to notify the end of a process
            EXECUTE FORMAT('DROP TRIGGER IF EXISTS log_%s_%s_end ON %s.%s;',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            EXECUTE FORMAT('CREATE TRIGGER log_%s_%s_end AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s.%s FOR EACH STATEMENT EXECUTE FUNCTION log.log_process()',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            END LOOP;
        END;
        $$;
        