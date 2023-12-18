

        CALL log.log_script('update_database', 'update_categories_juridiques_insee', 'BEGIN');
            


        CREATE TEMPORARY TABLE update_categories_juridiques_insee (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    

        TRUNCATE TABLE update_categories_juridiques_insee CASCADE;
        COPY update_categories_juridiques_insee
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        MERGE INTO public.categories_juridiques_insee
        USING update_categories_juridiques_insee
            ON public.categories_juridiques_insee.code_categorie_juridique_insee = update_categories_juridiques_insee.code
        WHEN MATCHED THEN
            UPDATE SET libelle = update_categories_juridiques_insee.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                code_categorie_juridique_insee,
                libelle
            ) VALUES (
                update_categories_juridiques_insee.code,
                update_categories_juridiques_insee.libelle
            );
    

        DROP TABLE update_categories_juridiques_insee;
        

        CALL log.log_script('update_database', 'update_categories_juridiques_insee', 'END');
            