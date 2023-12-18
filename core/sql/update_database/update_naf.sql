

        CALL log.log_script('update_database', 'update_naf', 'BEGIN');
            


        CREATE TEMPORARY TABLE update_naf (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    

        TRUNCATE TABLE update_naf CASCADE;
        COPY update_naf
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        MERGE INTO public.naf
        USING update_naf
            ON public.naf.code_naf = update_naf.code
        WHEN MATCHED THEN
            UPDATE SET libelle = update_naf.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                code_naf,
                libelle
            ) VALUES (
                update_naf.code,
                update_naf.libelle
            );
    

        DROP TABLE update_naf;
        

        CALL log.log_script('update_database', 'update_naf', 'END');
            