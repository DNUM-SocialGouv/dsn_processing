

        CALL log.log_script('monthly_integration', 'load_postes', 'BEGIN');
            

        WITH source_postes AS (
            SELECT
                UPPER(TRIM(libelleemploi)) AS libelle,
                MAX(datedeclaration) AS date_derniere_declaration
            FROM raw.raw_contrats
            GROUP BY UPPER(TRIM(libelleemploi))
        )

        INSERT INTO public.postes (
            poste_key,
            date_derniere_declaration
        )
        SELECT
            libelle,
            date_derniere_declaration
        FROM source_postes
        ON CONFLICT ON CONSTRAINT uk_poste
        DO UPDATE SET date_derniere_declaration = excluded.date_derniere_declaration;
        

        CALL log.log_script('monthly_integration', 'load_postes', 'END');
            