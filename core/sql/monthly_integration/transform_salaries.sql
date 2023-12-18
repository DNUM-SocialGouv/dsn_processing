

        CALL log.log_script('monthly_integration', 'transform_salaries', 'BEGIN');
            

        TRUNCATE TABLE source.source_salaries CASCADE;
        INSERT INTO source.source_salaries (
            source_salarie_id,
            source_etablissement_id,
            salarie_key,
            nir,
            nom_famille,
            nom_usage,
            prenoms,
            date_naissance,
            lieu_naissance,
            date_derniere_declaration
        ) SELECT
            idindividu AS source_salarie_id,
            idetablissement AS source_etablissement_id,
            COALESCE(nirdeclare, CONCAT(UPPER(TRIM(nomfamille)), UPPER(TRIM(prenoms)), OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0))) AS salarie_key,
            CAST(nirdeclare AS BIGINT) AS nir,
            UPPER(TRIM(nomfamille)) AS nom_famille,
            UPPER(TRIM(nomusage)) AS nom_usage,
            UPPER(TRIM(prenoms)) AS prenoms,
            OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0) AS date_naissance,
            lieunaissance AS lieu_naissance,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_salaries;
        

        CALL log.log_script('monthly_integration', 'transform_salaries', 'END');
            