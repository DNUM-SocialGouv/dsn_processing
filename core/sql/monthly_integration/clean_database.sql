

        CALL log.log_script('monthly_integration', 'clean_database', 'BEGIN');
            

        DO $$ 
        DECLARE 
            table_name text; 
        BEGIN 
            FOR table_name IN (SELECT tables.table_name FROM information_schema.tables WHERE table_schema = 'raw') 
            LOOP 
                EXECUTE 'TRUNCATE TABLE raw.' || table_name || ' CASCADE'; 
            END LOOP; 
        END $$;

        DO $$ 
        DECLARE 
            table_name text; 
        BEGIN 
            FOR table_name IN (SELECT tables.table_name FROM information_schema.tables WHERE table_schema = 'source') 
            LOOP 
                EXECUTE 'TRUNCATE TABLE source.' || table_name || ' CASCADE'; 
            END LOOP; 
        END $$;
        

        CALL log.log_script('monthly_integration', 'clean_database', 'END');
            