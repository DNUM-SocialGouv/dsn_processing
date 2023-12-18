

        CALL log.log_script('monthly_integration', 'transform_activites', 'BEGIN');
            

        TRUNCATE TABLE source.source_activites CASCADE;
        INSERT INTO source.source_activites (
            source_remuneration_id,
            source_activite_id,
            source_contrat_id,
            date_debut_paie,
            date_fin_paie,
            type_heures,
            nombre_heures,
            date_derniere_declaration
        )
        SELECT
            raw.raw_remunerations.idremuneration AS source_remuneration_id,
            raw.raw_activites.idactivite AS source_activite_id,
            raw.raw_contrats.idcontrat AS source_contrat_id,
            raw.raw_remunerations.datedebutperiodepaie AS date_debut_paie,
            raw.raw_remunerations.datefinperiodepaie AS date_fin_paie,
            CASE
                -- heures standards rémunérées
                WHEN raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.typeactivite = '01' THEN 1
                -- heures non rémunérées
                WHEN raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.typeactivite = '02' THEN 2
                -- heures supplémentaires rémunérées
                WHEN raw.raw_remunerations.typeremuneration IN ('017', '018') THEN 3
            END AS type_heures,
            CASE
                WHEN raw.raw_remunerations.typeremuneration = '002' THEN
                    CASE
                        WHEN raw.raw_activites.unitemesure IN ('10', '21') THEN raw.raw_activites.mesure
                        WHEN raw.raw_activites.unitemesure IN ('12', '20') THEN raw.raw_activites.mesure * 7
                        WHEN raw.raw_activites.unitemesure IS NULL AND raw.raw_contrats.codeunitequotite IN ('10', '21') THEN raw.raw_activites.mesure
                        WHEN raw.raw_activites.unitemesure IS NULL AND raw.raw_contrats.codeunitequotite IN ('12', '20') THEN raw.raw_activites.mesure * 7
                        ELSE 0
                    END
                WHEN raw.raw_remunerations.typeremuneration IN ('017', '018') THEN COALESCE(raw.raw_remunerations.nombreheure, 0)
            END AS nombre_heures,
            raw.raw_remunerations.datedeclaration AS date_derniere_declaration
        FROM raw.raw_remunerations
        LEFT JOIN raw.raw_activites
            ON raw.raw_remunerations.idremuneration = raw.raw_activites.idremuneration
        INNER JOIN raw.raw_versements
            ON raw.raw_versements.idversement = raw.raw_remunerations.idversement
        INNER JOIN raw.raw_contrats
            ON raw.raw_contrats.idindividu = raw.raw_versements.idindividu
            AND raw.raw_contrats.numero = raw.raw_remunerations.numerocontrat
        WHERE (raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.idactivite IS NOT NULL)
            OR raw.raw_remunerations.typeremuneration IN ('017', '018');
        

        CALL log.log_script('monthly_integration', 'transform_activites', 'END');
            