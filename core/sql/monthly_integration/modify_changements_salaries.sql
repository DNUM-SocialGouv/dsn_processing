

        CALL log.log_script('monthly_integration', 'modify_changements_salaries', 'BEGIN');
            


        TRUNCATE TABLE source.map_changes_salaries CASCADE;

        -- to handle case where employee was declared without NIR and now with a NIR
        INSERT INTO source.map_changes_salaries (
            old_salarie_id,
            new_salarie_id,
            date_modification
        )

        SELECT
            old_.salarie_id AS old_salarie_id,
            new_.salarie_id AS new_salarie_id,
            new_.date_derniere_declaration AS date_modification
        FROM public.salaries AS new_
        INNER JOIN public.salaries AS old_
            ON new_.salarie_id != old_.salarie_id
            AND new_.etablissement_id = old_.etablissement_id
            AND old_.nir IS NULL
            AND new_.nom_famille = old_.nom_famille
            AND new_.prenoms = old_.prenoms
            AND new_.date_naissance = old_.date_naissance
            AND new_.date_derniere_declaration >= old_.date_derniere_declaration
        ORDER BY new_.date_derniere_declaration DESC;


        -- add declared changes
        INSERT INTO source.map_changes_salaries (
            old_salarie_id,
            new_salarie_id,
            date_modification
        )

        SELECT
            salaries_old.salarie_id,
            salaries_new.salarie_id,
            changements_salaries.date_modification
        FROM source.source_changements_salaries AS changements_salaries
        INNER JOIN source.link_salaries AS link
            ON changements_salaries.source_salarie_id = link.source_salarie_id
        INNER JOIN public.salaries AS salaries_new
            ON salaries_new.salarie_id = link.salarie_id
        INNER JOIN public.salaries AS salaries_old
            ON salaries_new.etablissement_id = salaries_old.etablissement_id
            AND salaries_new.salarie_id != salaries_old.salarie_id
            AND (
                COALESCE(changements_salaries.ancien_nir, salaries_new.nir) = salaries_old.nir
                AND (
                    COALESCE(changements_salaries.ancien_nom_famille, salaries_new.nom_famille) = salaries_old.nom_famille
                    AND COALESCE(changements_salaries.anciens_prenoms, salaries_new.prenoms) = salaries_old.prenoms
                    AND COALESCE(changements_salaries.ancienne_date_naissance, salaries_new.date_naissance) = salaries_old.date_naissance
                )
            )
        WHERE changements_salaries.ancien_nir IS NOT NULL
            OR changements_salaries.ancien_nom_famille IS NOT NULL
            OR changements_salaries.anciens_prenoms IS NOT NULL
            OR changements_salaries.ancienne_date_naissance IS NOT NULL
        ORDER BY changements_salaries.date_modification DESC;
        

        -- remove isomorphic mappings (a -> a)
        DELETE FROM source.map_changes_salaries
        WHERE old_salarie_id = new_salarie_id;
        


        -- remove cyclic mappings (a -> b -> c -> a)
        WITH RECURSIVE search_graph(old_salarie_id,
                                    new_salarie_id,
                                    date_modification) AS (
            SELECT
                old_salarie_id,
                new_salarie_id,
                date_modification
            FROM source.map_changes_salaries
            UNION ALL
            SELECT
                graph.old_salarie_id,
                graph.new_salarie_id,
                graph.date_modification
            FROM source.map_changes_salaries AS graph, search_graph
            WHERE graph.old_salarie_id = search_graph.new_salarie_id
        )

        CYCLE new_salarie_id SET is_cycle USING path,

        qualified_paths AS (
            SELECT
                *,
                (SELECT MIN(path_as_array) FROM UNNEST(STRING_TO_ARRAY(TRANSLATE(CAST(path AS VARCHAR), '(){}', ''), ',')) AS path_as_array) AS min_vertex_path
            FROM search_graph
            WHERE is_cycle IS TRUE
        ),

        mapping_to_delete AS (
            SELECT
                old_salarie_id,
                new_salarie_id
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY min_vertex_path ORDER BY date_modification ASC) AS row_num
                FROM qualified_paths
            ) AS ord
            WHERE row_num = 1
        )

        DELETE FROM source.map_changes_salaries AS graph
        USING mapping_to_delete AS del
        WHERE graph.old_salarie_id = del.old_salarie_id
            AND graph.new_salarie_id = del.new_salarie_id;
        


        -- update the paths in the map table
        -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> d ; b -> d ; c -> d)
        DO $$
        DECLARE
            counter integer := 0;
            still_updating boolean := true;
        BEGIN
        WHILE still_updating LOOP
            -- update the paths by one step
            -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> c ; b -> d ; c -> d)
            WITH update_one_step AS (
                UPDATE source.map_changes_salaries AS graph
                SET new_salarie_id = graph_bis.new_salarie_id
                FROM source.map_changes_salaries AS graph_bis
                WHERE graph_bis.old_salarie_id = graph.new_salarie_id
                    AND graph_bis.old_salarie_id != graph_bis.new_salarie_id
                RETURNING 1)

            SELECT COUNT(*) > 0 INTO still_updating
            FROM update_one_step;
            -- if while loop is kind of infinite, we raise an error
            IF counter > 10000 THEN
                RAISE EXCEPTION 'infinite loop';
                EXIT;
            END IF;
            counter := counter + 1;
        END LOOP;
        END $$;
        


        -- keeps only one among a --> b ; a --> b; a --> c ; a --> d etc.
        WITH duplicates AS (
            SELECT
                ctid,
                old_salarie_id,
                ROW_NUMBER() OVER (PARTITION BY old_salarie_id ORDER BY date_modification DESC) AS nb
            FROM source.map_changes_salaries
        )

        DELETE FROM source.map_changes_salaries
        USING duplicates
        WHERE duplicates.old_salarie_id = source.map_changes_salaries.old_salarie_id
            AND duplicates.ctid = source.map_changes_salaries.ctid
            AND nb > 1;
        

        -- map table on contrats to reflect changes on salaries to contrats
        TRUNCATE TABLE source.map_changes_salaries_impact_contrats CASCADE;
        INSERT INTO source.map_changes_salaries_impact_contrats (
            old_contrat_id,
            new_contrat_id,
            date_modification
        )
        SELECT
            contrats_old.contrat_id,
            COALESCE(contrats_new.contrat_id, contrats_old.contrat_id) AS absorbing_id_column,
            map_changes_salaries.date_modification
        FROM source.map_changes_salaries AS map_changes_salaries
        INNER JOIN public.contrats AS contrats_old
            ON map_changes_salaries.old_salarie_id = contrats_old.salarie_id
        LEFT JOIN public.contrats AS contrats_new
            ON map_changes_salaries.new_salarie_id = contrats_new.salarie_id
            AND contrats_new.contrat_key = contrats_old.contrat_key;

        -- patch to handle case salarie1 -> salarie3 and salarie2 -> salarie3 and salarie1 and salarie2 have "same" contrat
        WITH ordered_changes_contrats AS (
            SELECT
                contrats_old.contrat_id,
                map_changes_salaries.new_salarie_id,
                contrats_old.contrat_key AS old_contrat_key,
                map_changes_salaries.date_modification,
                ROW_NUMBER() OVER(PARTITION BY map_changes_salaries.new_salarie_id, contrats_old.contrat_key ORDER BY map_changes_salaries.date_modification ASC) AS change_nb
            FROM source.map_changes_salaries AS map_changes_salaries
            INNER JOIN public.contrats AS contrats_old
                ON map_changes_salaries.old_salarie_id = contrats_old.salarie_id
        )

        INSERT INTO source.map_changes_salaries_impact_contrats (
            old_contrat_id,
            new_contrat_id,
            date_modification
        )
        SELECT
            merged.contrat_id AS old_contrat_id,
            absorbing.contrat_id AS new_contrat_id,
            merged.date_modification
        FROM ordered_changes_contrats AS merged
        INNER JOIN ordered_changes_contrats AS absorbing
            ON merged.new_salarie_id = absorbing.new_salarie_id
            AND merged.old_contrat_key = absorbing.old_contrat_key
            AND merged.change_nb = absorbing.change_nb + 1;
        

        -- remove isomorphic mappings (a -> a)
        DELETE FROM source.map_changes_salaries_impact_contrats
        WHERE old_contrat_id = new_contrat_id;
        


        -- remove cyclic mappings (a -> b -> c -> a)
        WITH RECURSIVE search_graph(old_contrat_id,
                                    new_contrat_id,
                                    date_modification) AS (
            SELECT
                old_contrat_id,
                new_contrat_id,
                date_modification
            FROM source.map_changes_salaries_impact_contrats
            UNION ALL
            SELECT
                graph.old_contrat_id,
                graph.new_contrat_id,
                graph.date_modification
            FROM source.map_changes_salaries_impact_contrats AS graph, search_graph
            WHERE graph.old_contrat_id = search_graph.new_contrat_id
        )

        CYCLE new_contrat_id SET is_cycle USING path,

        qualified_paths AS (
            SELECT
                *,
                (SELECT MIN(path_as_array) FROM UNNEST(STRING_TO_ARRAY(TRANSLATE(CAST(path AS VARCHAR), '(){}', ''), ',')) AS path_as_array) AS min_vertex_path
            FROM search_graph
            WHERE is_cycle IS TRUE
        ),

        mapping_to_delete AS (
            SELECT
                old_contrat_id,
                new_contrat_id
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY min_vertex_path ORDER BY date_modification ASC) AS row_num
                FROM qualified_paths
            ) AS ord
            WHERE row_num = 1
        )

        DELETE FROM source.map_changes_salaries_impact_contrats AS graph
        USING mapping_to_delete AS del
        WHERE graph.old_contrat_id = del.old_contrat_id
            AND graph.new_contrat_id = del.new_contrat_id;
        


        -- update the paths in the map table
        -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> d ; b -> d ; c -> d)
        DO $$
        DECLARE
            counter integer := 0;
            still_updating boolean := true;
        BEGIN
        WHILE still_updating LOOP
            -- update the paths by one step
            -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> c ; b -> d ; c -> d)
            WITH update_one_step AS (
                UPDATE source.map_changes_salaries_impact_contrats AS graph
                SET new_contrat_id = graph_bis.new_contrat_id
                FROM source.map_changes_salaries_impact_contrats AS graph_bis
                WHERE graph_bis.old_contrat_id = graph.new_contrat_id
                    AND graph_bis.old_contrat_id != graph_bis.new_contrat_id
                RETURNING 1)

            SELECT COUNT(*) > 0 INTO still_updating
            FROM update_one_step;
            -- if while loop is kind of infinite, we raise an error
            IF counter > 10000 THEN
                RAISE EXCEPTION 'infinite loop';
                EXIT;
            END IF;
            counter := counter + 1;
        END LOOP;
        END $$;
        


        -- keeps only one among a --> b ; a --> b; a --> c ; a --> d etc.
        WITH duplicates AS (
            SELECT
                ctid,
                old_contrat_id,
                ROW_NUMBER() OVER (PARTITION BY old_contrat_id ORDER BY date_modification DESC) AS nb
            FROM source.map_changes_salaries_impact_contrats
        )

        DELETE FROM source.map_changes_salaries_impact_contrats
        USING duplicates
        WHERE duplicates.old_contrat_id = source.map_changes_salaries_impact_contrats.old_contrat_id
            AND duplicates.ctid = source.map_changes_salaries_impact_contrats.ctid
            AND nb > 1;
        

        WITH activites_old AS (
            SELECT
                source.map_changes_salaries_impact_contrats.new_contrat_id,
                public.activites.mois,
                SUM(public.activites.heures_standards_remunerees) AS heures_standards_remunerees,
                SUM(public.activites.heures_non_remunerees) AS heures_non_remunerees,
                SUM(public.activites.heures_supp_remunerees) AS heures_supp_remunerees,
                MAX(public.activites.date_derniere_declaration) AS date_derniere_declaration
            FROM public.activites
            INNER JOIN source.map_changes_salaries_impact_contrats
                ON public.activites.contrat_id = source.map_changes_salaries_impact_contrats.old_contrat_id
            GROUP BY source.map_changes_salaries_impact_contrats.new_contrat_id, public.activites.mois
        )
        MERGE INTO activites AS activites_new
        USING activites_old
            ON activites_new.contrat_id = activites_old.new_contrat_id
            AND activites_new.mois = activites_old.mois
        WHEN NOT MATCHED THEN
            INSERT (
                contrat_id,
                mois,
                heures_standards_remunerees,
                heures_non_remunerees,
                heures_supp_remunerees,
                date_derniere_declaration
            ) VALUES (
                activites_old.new_contrat_id,
                activites_old.mois,
                activites_old.heures_standards_remunerees,
                activites_old.heures_non_remunerees,
                activites_old.heures_supp_remunerees,
                activites_old.date_derniere_declaration
            )
        WHEN MATCHED THEN
        UPDATE SET
        heures_standards_remunerees = activites_new.heures_standards_remunerees + activites_old.heures_standards_remunerees,
        heures_non_remunerees = activites_new.heures_non_remunerees + activites_old.heures_non_remunerees,
        heures_supp_remunerees = activites_new.heures_supp_remunerees + activites_old.heures_supp_remunerees,
        date_derniere_declaration = GREATEST(activites_new.date_derniere_declaration, activites_old.date_derniere_declaration);

        DELETE FROM activites
        USING source.map_changes_salaries_impact_contrats
        WHERE contrat_id = source.map_changes_salaries_impact_contrats.old_contrat_id;
    

        UPDATE source.link_contrats
        SET contrat_id = m.new_contrat_id
        FROM source.map_changes_salaries_impact_contrats AS m
        WHERE m.old_contrat_id = contrat_id
            AND m.old_contrat_id != m.new_contrat_id;
    

        UPDATE source.link_salaries
        SET salarie_id = m.new_salarie_id
        FROM source.map_changes_salaries AS m
        WHERE m.old_salarie_id = salarie_id
            AND m.old_salarie_id != m.new_salarie_id;
    

        DELETE FROM public.contrats
        USING source.map_changes_salaries_impact_contrats AS m
        WHERE contrat_id = m.old_contrat_id
            AND m.old_contrat_id != m.new_contrat_id;
    

        UPDATE public.contrats
        SET salarie_id = m.new_salarie_id
        FROM source.map_changes_salaries AS m
        WHERE m.old_salarie_id = salarie_id
            AND m.old_salarie_id != m.new_salarie_id;
    

        DELETE FROM public.salaries
        USING source.map_changes_salaries AS m
        WHERE salarie_id = m.old_salarie_id
            AND m.old_salarie_id != m.new_salarie_id;
    

        CALL log.log_script('monthly_integration', 'modify_changements_salaries', 'END');
            