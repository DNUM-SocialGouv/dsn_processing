

        CALL log.log_script('monthly_integration', 'transform_contrats', 'BEGIN');
            

        TRUNCATE TABLE source.source_contrats CASCADE;
        INSERT INTO source.source_contrats (
            source_contrat_id,
            source_etablissement_id,
            source_salarie_id,
            contrat_key,
            numero,
            date_debut,
            poste_id,
            code_nature_contrat,
            code_convention_collective,
            code_motif_recours,
            code_dispositif_public,
            code_complement_dispositif_public,
            lieu_travail,
            code_mise_disposition_externe,
            date_fin_previsionnelle,
            etu_etablissement_key,
            etu_id,
            date_derniere_declaration
        ) SELECT
            raw.raw_contrats.idcontrat AS source_contrat_id,
            source.source_salaries.source_etablissement_id AS source_etablissement_id,
            raw.raw_contrats.idindividu AS source_salarie_id,
            CONCAT(UPPER(TRIM(raw.raw_contrats.numero)), '_', CAST(raw.raw_contrats.datedebut AS CHAR(10))) AS contrat_key,
            UPPER(TRIM(raw.raw_contrats.numero)) AS numero,
            raw.raw_contrats.datedebut AS date_debut,
            public.postes.poste_id AS poste_id,
            raw.raw_contrats.codenature AS code_nature_contrat,
            raw.raw_contrats.codeccn AS code_convention_collective,
            raw.raw_contrats.codemotifrecours AS code_motif_recours,
            raw.raw_contrats.codedisppolitiquepublique AS code_dispositif_public,
            raw.raw_contrats.compltdispositifpublic AS code_complement_dispositif_public,
            raw.raw_contrats.lieutravail AS lieu_travail,
            raw.raw_contrats.misedispoexterneindividu AS code_mise_disposition_externe,
            raw.raw_contrats.datefinprev AS date_fin_previsionnelle,
            CASE WHEN (LENGTH(raw.raw_contrats.siretetabutilisateur) = 14
                AND REGEXP_LIKE(raw.raw_contrats.siretetabutilisateur, '^[0-9]*$'))
                THEN CAST(raw.raw_contrats.siretetabutilisateur AS BIGINT)
            END AS etu_etablissement_key,
            NULL AS etu_id,
            raw.raw_contrats.datedeclaration AS date_derniere_declaration
        FROM raw.raw_contrats
        INNER JOIN source.source_salaries
            ON raw.raw_contrats.idindividu = source.source_salaries.source_salarie_id
        INNER JOIN public.postes
            ON public.postes.poste_key = UPPER(TRIM(raw.raw_contrats.libelleemploi));

        -- Ajout de l'id de l'ETU dans la table source si le siret renseigné match un siret de la table etablissements
        UPDATE source.source_contrats
        SET
            etu_id = public.etablissements.etablissement_id
        FROM public.etablissements
        WHERE source.source_contrats.etu_etablissement_key = public.etablissements.etablissement_key;
        

        CALL log.log_script('monthly_integration', 'transform_contrats', 'END');
            