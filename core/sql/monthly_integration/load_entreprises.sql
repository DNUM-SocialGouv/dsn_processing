

        CALL log.log_script('monthly_integration', 'load_entreprises', 'BEGIN');
            

        -- On importe les données sans doublon
        WITH sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY entreprise_key ORDER BY date_derniere_declaration DESC) AS row_num
                FROM source.source_entreprises
            ) AS ord
            WHERE row_num = 1
        )

        -- On merge les données entrantes dans la table permanente sur la base de la key_column
        MERGE INTO public.entreprises AS target
        USING sourcewithoutduplicates AS source
            ON target.entreprise_key = source.entreprise_key
        WHEN NOT MATCHED THEN
            -- Si aucune ligne avec la key_column considérée n'existe, on insert la donnée
            INSERT (entreprise_key,
                    raison_sociale,date_derniere_declaration
                    , date_premiere_declaration)
            VALUES (source.entreprise_key,
                     source.raison_sociale, source.date_derniere_declaration
                    , source.date_derniere_declaration)
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            -- Sinon on update les colonnes auxiliaires
            UPDATE SET entreprise_key = source.entreprise_key, raison_sociale = source.raison_sociale, date_derniere_declaration = source.date_derniere_declaration;


        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        TRUNCATE TABLE source.link_entreprises CASCADE;
        INSERT INTO source.link_entreprises (source_etablissement_id, entreprise_id)
        SELECT
            source.source_etablissement_id,
            target.entreprise_id
        FROM source.source_entreprises AS source
        LEFT JOIN public.entreprises AS target
            ON source.entreprise_key = target.entreprise_key;
    

        CALL log.log_script('monthly_integration', 'load_entreprises', 'END');
            