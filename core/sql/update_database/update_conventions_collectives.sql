

        CALL log.log_script('update_database', 'update_conventions_collectives', 'BEGIN');
            


        CREATE TEMPORARY TABLE update_conventions_collectives (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    

        TRUNCATE TABLE update_conventions_collectives CASCADE;
        COPY update_conventions_collectives
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        MERGE INTO public.conventions_collectives
        USING update_conventions_collectives
            ON public.conventions_collectives.code_convention_collective = update_conventions_collectives.code
        WHEN MATCHED THEN
            UPDATE SET libelle = update_conventions_collectives.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                code_convention_collective,
                libelle
            ) VALUES (
                update_conventions_collectives.code,
                update_conventions_collectives.libelle
            );
    

        DROP TABLE update_conventions_collectives;
        

        CALL log.log_script('update_database', 'update_conventions_collectives', 'END');
            