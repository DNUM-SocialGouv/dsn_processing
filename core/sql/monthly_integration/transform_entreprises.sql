

        CALL log.log_script('monthly_integration', 'transform_entreprises', 'BEGIN');
            

        TRUNCATE TABLE source.source_entreprises CASCADE;
        INSERT INTO source.source_entreprises (
            source_etablissement_id,
            entreprise_key,
            raison_sociale,
            date_derniere_declaration
        ) SELECT
            idetablissement AS source_etablissement_id,
            CAST(siren AS BIGINT) AS entreprise_key,
            raisonsocialeentr AS raison_sociale,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_etablissements;
        

        CALL log.log_script('monthly_integration', 'transform_entreprises', 'END');
            