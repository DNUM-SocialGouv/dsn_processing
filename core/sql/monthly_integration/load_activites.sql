

        CALL log.log_script('monthly_integration', 'load_activites', 'BEGIN');
            

        WITH unique_periods AS (
            SELECT
                date_debut_paie,
                date_fin_paie
            FROM source.source_activites
            WHERE date_fin_paie >= date_debut_paie
            GROUP BY (date_debut_paie, date_fin_paie)
        )
        , one_row_per_month AS (
            SELECT
                GENERATE_SERIES(DATE_TRUNC('month', date_debut_paie)::DATE,
                                DATE_TRUNC('month', date_fin_paie)::DATE,
                                '1 month'::INTERVAL)::DATE AS mois,
                date_debut_paie,
                date_fin_paie
            FROM unique_periods
        )
        , monthly_cut AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                GREATEST(date_debut_paie, mois) AS monthly_debut_periode_paie,
                LEAST(date_fin_paie, (mois + INTERVAL '1 MONTH - 1 DAY')::DATE) AS monthly_fin_periode_paie
            FROM one_row_per_month
        )
        , monthly_nb AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                COUNT(CASE WHEN weekday_number NOT IN (0,6) AND public_holiday IS NOT TRUE THEN 1 END) AS nb_working_days
            FROM monthly_cut
            INNER JOIN public.daily_calendar
                ON monthly_cut.monthly_debut_periode_paie <= public.daily_calendar.full_date
                AND monthly_cut.monthly_fin_periode_paie >= public.daily_calendar.full_date
            GROUP BY (date_debut_paie, date_fin_paie, mois)
        )
        , monhtly_and_total_nb AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                nb_working_days,
                SUM(nb_working_days) OVER (PARTITION BY date_debut_paie, date_fin_paie) AS div_working_days
            FROM monthly_nb
        )
        , monthly_ratio AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                CASE WHEN div_working_days != 0 THEN nb_working_days / div_working_days ELSE 1 END AS ratio
            FROM monhtly_and_total_nb
        )
        , linked_activites AS (
            SELECT
                source.link_contrats.contrat_id,
                date_debut_paie,
                date_fin_paie,
                type_heures,
                nombre_heures,
                date_derniere_declaration
            FROM source.source_activites
            INNER JOIN source.link_contrats
                ON source.link_contrats.source_contrat_id = source.source_activites.source_contrat_id
        )
        , new_activites AS (
            SELECT
                linked_activites.contrat_id,
                monthly_ratio.mois,
                SUM(CASE WHEN linked_activites.type_heures = 1 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_standards_remunerees,
                SUM(CASE WHEN linked_activites.type_heures = 2 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_non_remunerees,
                SUM(CASE WHEN linked_activites.type_heures = 3 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_supp_remunerees,
                MAX(linked_activites.date_derniere_declaration) AS date_derniere_declaration
            FROM linked_activites
            INNER JOIN monthly_ratio
                ON linked_activites.date_debut_paie = monthly_ratio.date_debut_paie
                AND linked_activites.date_fin_paie = monthly_ratio.date_fin_paie
            GROUP BY (linked_activites.contrat_id, monthly_ratio.mois)
        )
        MERGE INTO public.activites
        USING new_activites
            ON new_activites.contrat_id = public.activites.contrat_id
            AND new_activites.mois = public.activites.mois
        WHEN NOT MATCHED THEN
            INSERT (
                contrat_id,
                mois,
                heures_standards_remunerees,
                heures_non_remunerees,
                heures_supp_remunerees,
                date_derniere_declaration
            )
            VALUES (
                new_activites.contrat_id,
                new_activites.mois,
                new_activites.heures_standards_remunerees,
                new_activites.heures_non_remunerees,
                new_activites.heures_supp_remunerees,
                new_activites.date_derniere_declaration
            )
        WHEN MATCHED THEN
        UPDATE SET
        heures_standards_remunerees = public.activites.heures_standards_remunerees + new_activites.heures_standards_remunerees,
        heures_non_remunerees = public.activites.heures_non_remunerees + new_activites.heures_non_remunerees,
        heures_supp_remunerees = public.activites.heures_supp_remunerees + new_activites.heures_supp_remunerees,
        date_derniere_declaration = GREATEST(public.activites.date_derniere_declaration, new_activites.date_derniere_declaration);
        

        CALL log.log_script('monthly_integration', 'load_activites', 'END');
            