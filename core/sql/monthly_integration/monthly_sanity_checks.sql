

        CALL log.log_script('monthly_integration', 'monthly_sanity_checks', 'BEGIN');
            


        DO $$
        DECLARE
        do_trinomial_indexes_are_consistent integer;
        BEGIN
            SELECT COUNT(*)
            INTO do_trinomial_indexes_are_consistent
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON c.salarie_id = s.salarie_id
            WHERE c.etablissement_id != s.etablissement_id;

        ASSERT do_trinomial_indexes_are_consistent = 0, 'Inconsistance in trinomial indexes.
         One contrat refers to a salarie which does not have the same etablissement_id.';
        END$$;
        

        DO $$
        DECLARE do_source_link_entreprises_ids_are_consistent integer;
        BEGIN
            SELECT SUM(CASE WHEN T.entreprise_id IS NULL THEN 1 ELSE 0 END)
            INTO do_source_link_entreprises_ids_are_consistent
            FROM source.link_entreprises AS link
            LEFT JOIN public.entreprises AS T
                ON link.entreprise_id = T.entreprise_id;

        ASSERT do_source_link_entreprises_ids_are_consistent = 0, 'source.link_entreprises has one id referenced which does not exist';
        END$$;
    

        DO $$
        DECLARE do_source_link_etablissements_ids_are_consistent integer;
        BEGIN
            SELECT SUM(CASE WHEN T.etablissement_id IS NULL THEN 1 ELSE 0 END)
            INTO do_source_link_etablissements_ids_are_consistent
            FROM source.link_etablissements AS link
            LEFT JOIN public.etablissements AS T
                ON link.etablissement_id = T.etablissement_id;

        ASSERT do_source_link_etablissements_ids_are_consistent = 0, 'source.link_etablissements has one id referenced which does not exist';
        END$$;
    

        DO $$
        DECLARE do_source_link_salaries_ids_are_consistent integer;
        BEGIN
            SELECT SUM(CASE WHEN T.salarie_id IS NULL THEN 1 ELSE 0 END)
            INTO do_source_link_salaries_ids_are_consistent
            FROM source.link_salaries AS link
            LEFT JOIN public.salaries AS T
                ON link.salarie_id = T.salarie_id;

        ASSERT do_source_link_salaries_ids_are_consistent = 0, 'source.link_salaries has one id referenced which does not exist';
        END$$;
    

        DO $$
        DECLARE do_source_link_contrats_ids_are_consistent integer;
        BEGIN
            SELECT SUM(CASE WHEN T.contrat_id IS NULL THEN 1 ELSE 0 END)
            INTO do_source_link_contrats_ids_are_consistent
            FROM source.link_contrats AS link
            LEFT JOIN public.contrats AS T
                ON link.contrat_id = T.contrat_id;

        ASSERT do_source_link_contrats_ids_are_consistent = 0, 'source.link_contrats has one id referenced which does not exist';
        END$$;
    

        CALL log.log_script('monthly_integration', 'monthly_sanity_checks', 'END');
            