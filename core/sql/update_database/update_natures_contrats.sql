

        CALL log.log_script('update_database', 'update_natures_contrats', 'BEGIN');
            


        CREATE TEMPORARY TABLE update_natures_contrats (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    

        TRUNCATE TABLE update_natures_contrats CASCADE;
        COPY update_natures_contrats
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        MERGE INTO public.natures_contrats
        USING update_natures_contrats
            ON public.natures_contrats.code_nature_contrat = update_natures_contrats.code
        WHEN MATCHED THEN
            UPDATE SET libelle = update_natures_contrats.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                code_nature_contrat,
                libelle
            ) VALUES (
                update_natures_contrats.code,
                update_natures_contrats.libelle
            );
    

        DROP TABLE update_natures_contrats;
        

        CALL log.log_script('update_database', 'update_natures_contrats', 'END');
            