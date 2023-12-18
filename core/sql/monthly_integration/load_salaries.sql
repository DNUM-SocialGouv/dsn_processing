

        CALL log.log_script('monthly_integration', 'load_salaries', 'BEGIN');
            

        -- On importe les données avec les id "généalogiques" de la table grandparent permanente
        WITH sourcewithdependences AS (
            SELECT
                source.source_salaries.*,
                link.etablissement_id AS grandparent
            FROM source.source_salaries
            LEFT JOIN source.link_etablissements AS link
                ON link.source_etablissement_id = source.source_salaries.source_etablissement_id
        ),

        -- On supprime les doublons des données
        sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY grandparent, salarie_key ORDER BY date_derniere_declaration DESC) AS row_num
                FROM sourcewithdependences
            ) AS ord
            WHERE row_num = 1
        )
        -- On merge les données entrantes dans la table permanente sur la base de la key_column et des id de la table grandparent
        MERGE INTO public.salaries AS target
        USING sourcewithoutduplicates AS source
            ON (target.etablissement_id = source.grandparent)
            AND (target.salarie_key = source.salarie_key)
        -- Si la donnée n'est pas présente dans la table, on l'insert
        WHEN NOT MATCHED THEN
            INSERT (etablissement_id,
                    salarie_key,
                    nir,nom_famille,nom_usage,prenoms,date_naissance,lieu_naissance,date_derniere_declaration
                    )
            VALUES (source.grandparent,
                    source.salarie_key,
                     source.nir, source.nom_famille, source.nom_usage, source.prenoms, source.date_naissance, source.lieu_naissance, source.date_derniere_declaration
                    )
        -- Sinon, on update les informations auxiliaires
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            UPDATE SET salarie_key = source.salarie_key, nir = source.nir, nom_famille = source.nom_famille, nom_usage = source.nom_usage, prenoms = source.prenoms, date_naissance = source.date_naissance, lieu_naissance = source.lieu_naissance, date_derniere_declaration = source.date_derniere_declaration;

        -- A nouveau, on considère les données entrantes avec les id "généalogiques" de la table grandparent permanente
        TRUNCATE TABLE source.link_salaries CASCADE;
        WITH sourcewithdependences AS (
            SELECT
                source.source_salaries.source_salarie_id,
                source.source_salaries.salarie_key,
                link.etablissement_id AS grandparent
            FROM source.source_salaries
            LEFT JOIN source.link_etablissements AS link
                ON link.source_etablissement_id = source.source_salaries.source_etablissement_id
        )

        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        INSERT INTO source.link_salaries (source_salarie_id, salarie_id)
        SELECT
            source.source_salarie_id,
            target.salarie_id
        FROM sourcewithdependences AS source
        LEFT JOIN public.salaries AS target
            ON (target.etablissement_id = source.grandparent)
            AND (source.salarie_key = target.salarie_key);
    

        CALL log.log_script('monthly_integration', 'load_salaries', 'END');
            