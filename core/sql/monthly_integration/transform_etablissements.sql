

        CALL log.log_script('monthly_integration', 'transform_etablissements', 'BEGIN');
            

        TRUNCATE TABLE source.source_etablissements CASCADE;
        INSERT INTO source.source_etablissements (
            source_etablissement_id,
            entreprise_key,
            etablissement_key,
            siege_social,
            enseigne,
            adresse,
            complement_adresse,
            code_postal,
            commune,
            code_categorie_juridique_insee,
            code_naf,
            code_convention_collective,
            date_derniere_declaration
        ) SELECT
            idetablissement AS source_etablissement_id,
            CAST(siren AS BIGINT) AS entreprise_key,
            CAST(CONCAT(siren, nic) AS BIGINT) AS etablissement_key,
            COALESCE(nicentre = nic, FALSE) AS siege_social,
            enseigneetablissement AS enseigne,
            voieetab AS adresse,
            compltvoieetab AS complement_adresse,
            cp AS commune_postal,
            localite AS commune,
            categoriejuridique AS code_categorie_juridique_insee,
            codeapet AS code_naf,
            codeconvcollectiveprinci AS code_convention_collective,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_etablissements;
        

        CALL log.log_script('monthly_integration', 'transform_etablissements', 'END');
            