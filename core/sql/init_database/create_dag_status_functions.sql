

        CREATE OR REPLACE FUNCTION SET_STATUS_TO_ONGOING()
        RETURNS VOID AS $$
        BEGIN
            IF (SELECT status <> 'SUCCESS' FROM sys.current_status) THEN
                RAISE EXCEPTION 'Last DAG let the database in non SUCCESS status. If you want to force execution, please run : UPDATE sys.current_status SET status = ''SUCCESS''.';
            ELSE
                UPDATE sys.current_status SET status = 'ONGOING';
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION SET_STATUS_TO_SUCCESS()
        RETURNS VOID AS $$
        BEGIN
            UPDATE sys.current_status SET status = 'SUCCESS';
        END;
        $$ LANGUAGE plpgsql;
        