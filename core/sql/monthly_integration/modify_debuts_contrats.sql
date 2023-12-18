

        CALL log.log_script('monthly_integration', 'modify_debuts_contrats', 'BEGIN');
            

        -- INFER EFFECTIVE START DATES IN CASE OF EMPLOYER CHANGES FOR NEW EMPLOYER'S CONTRATS
        -- get contrats which has been transfered (info in changements_contrats)
        WITH contrats_transferes AS (
            SELECT
                contrat_new.contrat_id,
                changements_contrats.date_modification AS date_debut_effective
            FROM source.source_changements_contrats AS changements_contrats
            INNER JOIN source.link_contrats AS link
                ON changements_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats AS contrat_new
                ON contrat_new.contrat_id = link.contrat_id
            INNER JOIN public.etablissements AS etablissement_new
                ON contrat_new.etablissement_id = etablissement_new.etablissement_id
            WHERE changements_contrats.siret_ancien_employeur IS NOT NULL
                AND etablissement_new.etablissement_key != changements_contrats.siret_ancien_employeur
                AND changements_contrats.date_modification != contrat_new.date_debut
        )

        -- set date_debut_effetive at the DATE of change in new employer
        UPDATE public.contrats
        SET date_debut_effective = contrats_transferes.date_debut_effective
        FROM contrats_transferes
        WHERE contrats_transferes.contrat_id = public.contrats.contrat_id;
        

        CALL log.log_script('monthly_integration', 'modify_debuts_contrats', 'END');
            