

        CALL log.log_script('monthly_integration', 'modify_fins_contrats', 'BEGIN');
            

        -- DECLARED END DATES
        -- get end date linked with contrat_id in the database
        WITH dates_fin AS (
            SELECT
                public.contrats.contrat_id,
                fins_contrats.date_fin AS date_fin,
                fins_contrats.code_motif_rupture AS motif_fin
            FROM source.source_fins_contrats AS fins_contrats
            INNER JOIN source.link_contrats AS link
                ON fins_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats
                ON link.contrat_id = public.contrats.contrat_id
        )

        -- update date_fin_effective, knowing that motif_fin = '099' means a cancelling of the end DATE
        UPDATE public.contrats
        SET date_fin_effective = CASE WHEN (fins.motif_fin = '099') THEN NULL ELSE fins.date_fin END,
            statut_fin = CASE WHEN fins.motif_fin = '099' THEN NULL ELSE 2 END
        FROM dates_fin AS fins
        WHERE public.contrats.contrat_id = fins.contrat_id;

        -- INFER END DATES BY LACK OF DECLARATIONS
        UPDATE public.contrats
        SET date_fin_effective = (
            CASE WHEN dernieres_declarations.date_fin_previsionnelle IS NOT NULL
                AND dernieres_declarations.date_fin_previsionnelle >= dernieres_declarations.premier_jour_mois_derniere_declaration_contrat
                AND dernieres_declarations.date_fin_previsionnelle <= dernieres_declarations.dernier_jour_mois_derniere_declaration_contrat
                THEN dernieres_declarations.date_fin_previsionnelle
                ELSE dernieres_declarations.dernier_jour_mois_derniere_declaration_contrat END
            ),
            statut_fin = 1
        FROM (
            SELECT
                public.contrats.contrat_id,
                public.contrats.date_derniere_declaration AS premier_jour_mois_derniere_declaration_contrat,
                CAST(DATE_TRUNC('MONTH', public.contrats.date_derniere_declaration) + INTERVAL '1 MONTH - 1 day' AS DATE) AS dernier_jour_mois_derniere_declaration_contrat,
                public.contrats.date_fin_previsionnelle,
                public.etablissements.date_derniere_declaration AS date_derniere_declaration_etablissement,
                public.etablissements.ouvert AS ouverture_etablissement
            FROM public.contrats
            INNER JOIN public.etablissements
                ON public.contrats.etablissement_id = public.etablissements.etablissement_id
        ) AS dernieres_declarations
        WHERE dernieres_declarations.contrat_id = public.contrats.contrat_id
            AND (dernieres_declarations.date_derniere_declaration_etablissement >= (public.contrats.date_derniere_declaration + INTERVAL '3 MONTH') OR dernieres_declarations.ouverture_etablissement IS FALSE)
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);

        -- REMOVE INFERED END DATES WITH NEW DECLARATIONS
        UPDATE public.contrats
        SET date_fin_effective = NULL,
            statut_fin = NULL
        FROM (
            SELECT
                public.contrats.contrat_id,
                public.contrats.date_derniere_declaration AS date_derniere_declaration_contrat,
                public.etablissements.date_derniere_declaration AS date_derniere_declaration_etablissement,
                public.etablissements.ouvert AS ouverture_etablissement
            FROM public.contrats
            INNER JOIN public.etablissements
                ON public.contrats.etablissement_id = public.etablissements.etablissement_id
        ) AS dernieres_declarations
        WHERE dernieres_declarations.contrat_id = public.contrats.contrat_id
            AND ouverture_etablissement IS TRUE
            AND dernieres_declarations.date_derniere_declaration_etablissement < (public.contrats.date_derniere_declaration + INTERVAL '3 MONTH')
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);

        -- INFER END DATES IN CASE OF EMPLOYER CHANGES FOR OLD EMPLOYER'S CONTRATS
        -- get contrats which has been transfered (info in changements_contrats)
        WITH contrats_transferes AS (
            SELECT
                CAST(changements_contrats.siret_ancien_employeur AS BIGINT) AS siret_ancien_employeur,
                CONCAT(
                    COALESCE(changements_contrats.ancien_numero, contrat_new.numero),
                    '_',
                    CAST(COALESCE(changements_contrats.ancienne_date_debut, contrat_new.date_debut) AS CHAR(10))
                ) AS ancienne_contrat_key,
                salarie_new.salarie_key AS ancienne_salarie_key,
                changements_contrats.date_modification AS date_modification
            FROM source.source_changements_contrats AS changements_contrats
            INNER JOIN source.link_contrats AS link
                ON changements_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats AS contrat_new
                ON contrat_new.contrat_id = link.contrat_id
            INNER JOIN public.salaries AS salarie_new
                ON salarie_new.salarie_id = contrat_new.salarie_id
            INNER JOIN public.etablissements AS etablissement_new
                ON contrat_new.etablissement_id = etablissement_new.etablissement_id
            WHERE changements_contrats.siret_ancien_employeur IS NOT NULL
                AND etablissement_new.etablissement_key != changements_contrats.siret_ancien_employeur
        ),

        -- infer end dates for contrats in old employer
        contrats_a_fermer AS (
            SELECT
                contrat_old.contrat_id,
                contrats_transferes.date_modification
            FROM contrats_transferes
            INNER JOIN public.etablissements AS etablissement_old
                ON etablissement_old.etablissement_key = contrats_transferes.siret_ancien_employeur
            INNER JOIN public.salaries AS salarie_old
                ON salarie_old.etablissement_id = etablissement_old.etablissement_id
                AND salarie_old.salarie_key = contrats_transferes.ancienne_salarie_key
            INNER JOIN public.contrats AS contrat_old
                ON contrat_old.salarie_id = salarie_old.salarie_id
                AND contrat_old.contrat_key = contrats_transferes.ancienne_contrat_key
        )

        -- set date_fin_effective at the date of employer changer
        UPDATE public.contrats
        SET date_fin_effective = date_modification - INTERVAL '1 day',
            statut_fin = 2
        FROM contrats_a_fermer
        WHERE contrats_a_fermer.contrat_id = public.contrats.contrat_id
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);
        

        CALL log.log_script('monthly_integration', 'modify_fins_contrats', 'END');
            