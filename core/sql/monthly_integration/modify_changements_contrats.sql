

        CALL log.log_script('monthly_integration', 'modify_changements_contrats', 'BEGIN');
            


        TRUNCATE TABLE source.map_changes_contrats CASCADE;
        INSERT INTO source.map_changes_contrats (
            old_contrat_id,
            new_contrat_id,
            date_modification
        )
        SELECT
            contrats_old.contrat_id AS id_old_row,
            contrats_new.contrat_id AS id_new_row,
            changements_contrats.date_modification
        FROM source.source_changements_contrats AS changements_contrats
        INNER JOIN source.link_contrats AS link
            ON changements_contrats.source_contrat_id = link.source_contrat_id
        INNER JOIN public.contrats AS contrats_new
            ON contrats_new.contrat_id = link.contrat_id
        INNER JOIN public.contrats AS contrats_old
            ON contrats_new.etablissement_id = contrats_old.etablissement_id
            AND contrats_new.salarie_id = contrats_old.salarie_id
            AND COALESCE(changements_contrats.ancienne_date_debut, contrats_new.date_debut) = contrats_old.date_debut
            AND COALESCE(changements_contrats.ancien_numero, contrats_new.numero) = contrats_old.numero
        WHERE changements_contrats.ancienne_date_debut IS NOT NULL
            OR changements_contrats.ancien_numero IS NOT NULL
        ORDER BY changements_contrats.date_modification;
        

        -- remove isomorphic mappings (a -> a)
        DELETE FROM source.map_changes_contrats
        WHERE old_contrat_id = new_contrat_id;
        


        DO $$ 
        DECLARE
            rows_deleted INTEGER;
        BEGIN
            LOOP
                -- remove cyclic mappings (a -> b -> c -> a)
                WITH RECURSIVE search_graph(old_contrat_id,
                                            new_contrat_id,
                                            date_modification) AS (
                    SELECT
                        old_contrat_id,
                        new_contrat_id,
                        date_modification
                    FROM source.map_changes_contrats
                    UNION ALL
                    SELECT
                        graph.old_contrat_id,
                        graph.new_contrat_id,
                        graph.date_modification
                    FROM source.map_changes_contrats AS graph, search_graph
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

                DELETE FROM source.map_changes_contrats AS graph
                USING mapping_to_delete AS del
                WHERE graph.old_contrat_id = del.old_contrat_id
                    AND graph.new_contrat_id = del.new_contrat_id;

                GET DIAGNOSTICS rows_deleted = ROW_COUNT;

                EXIT WHEN rows_deleted = 0;
            END LOOP;
        END $$;
        


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
                UPDATE source.map_changes_contrats AS graph
                SET new_contrat_id = graph_bis.new_contrat_id
                FROM source.map_changes_contrats AS graph_bis
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
            FROM source.map_changes_contrats
        )

        DELETE FROM source.map_changes_contrats
        USING duplicates
        WHERE duplicates.old_contrat_id = source.map_changes_contrats.old_contrat_id
            AND duplicates.ctid = source.map_changes_contrats.ctid
            AND nb > 1;
        

        WITH activites_old AS (
            SELECT
                source.map_changes_contrats.new_contrat_id,
                public.activites.mois,
                SUM(public.activites.heures_standards_remunerees) AS heures_standards_remunerees,
                SUM(public.activites.heures_non_remunerees) AS heures_non_remunerees,
                SUM(public.activites.heures_supp_remunerees) AS heures_supp_remunerees,
                MAX(public.activites.date_derniere_declaration) AS date_derniere_declaration
            FROM public.activites
            INNER JOIN source.map_changes_contrats
                ON public.activites.contrat_id = source.map_changes_contrats.old_contrat_id
            GROUP BY source.map_changes_contrats.new_contrat_id, public.activites.mois
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
        USING source.map_changes_contrats
        WHERE contrat_id = source.map_changes_contrats.old_contrat_id;
    

        UPDATE source.link_contrats
        SET contrat_id = m.new_contrat_id
        FROM source.map_changes_contrats AS m
        WHERE m.old_contrat_id = contrat_id
            AND m.old_contrat_id != m.new_contrat_id;
    

        DELETE FROM public.contrats
        USING source.map_changes_contrats AS m
        WHERE contrat_id = m.old_contrat_id
            AND m.old_contrat_id != m.new_contrat_id;
    

        CALL log.log_script('monthly_integration', 'modify_changements_contrats', 'END');
            