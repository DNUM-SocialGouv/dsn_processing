

        CALL log.log_script('monthly_integration', 'load_etablissements', 'BEGIN');
            

        -- On importe les données avec les id "généalogiques" de la table grandparent permanente
        WITH sourcewithdependences AS (
            SELECT
                source.source_etablissements.*,
                link.entreprise_id AS grandparent
            FROM source.source_etablissements
            LEFT JOIN source.link_entreprises AS link
                ON link.source_etablissement_id = source.source_etablissements.source_etablissement_id
        ),

        -- On supprime les doublons des données
        sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY grandparent, etablissement_key ORDER BY date_derniere_declaration DESC) AS row_num
                FROM sourcewithdependences
            ) AS ord
            WHERE row_num = 1
        )
        -- On merge les données entrantes dans la table permanente sur la base de la key_column et des id de la table grandparent
        MERGE INTO public.etablissements AS target
        USING sourcewithoutduplicates AS source
            ON (target.entreprise_id = source.grandparent)
            AND (target.etablissement_key = source.etablissement_key)
        -- Si la donnée n'est pas présente dans la table, on l'insert
        WHEN NOT MATCHED THEN
            INSERT (entreprise_id,
                    etablissement_key,
                    siege_social,enseigne,adresse,complement_adresse,code_postal,commune,code_categorie_juridique_insee,code_naf,code_convention_collective,date_derniere_declaration
                    , date_premiere_declaration)
            VALUES (source.grandparent,
                    source.etablissement_key,
                     source.siege_social, source.enseigne, source.adresse, source.complement_adresse, source.code_postal, source.commune, source.code_categorie_juridique_insee, source.code_naf, source.code_convention_collective, source.date_derniere_declaration
                    , source.date_derniere_declaration)
        -- Sinon, on update les informations auxiliaires
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            UPDATE SET etablissement_key = source.etablissement_key, siege_social = source.siege_social, enseigne = source.enseigne, adresse = source.adresse, complement_adresse = source.complement_adresse, code_postal = source.code_postal, commune = source.commune, code_categorie_juridique_insee = source.code_categorie_juridique_insee, code_naf = source.code_naf, code_convention_collective = source.code_convention_collective, date_derniere_declaration = source.date_derniere_declaration;

        -- A nouveau, on considère les données entrantes avec les id "généalogiques" de la table grandparent permanente
        TRUNCATE TABLE source.link_etablissements CASCADE;
        WITH sourcewithdependences AS (
            SELECT
                source.source_etablissements.source_etablissement_id,
                source.source_etablissements.etablissement_key,
                link.entreprise_id AS grandparent
            FROM source.source_etablissements
            LEFT JOIN source.link_entreprises AS link
                ON link.source_etablissement_id = source.source_etablissements.source_etablissement_id
        )

        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        INSERT INTO source.link_etablissements (source_etablissement_id, etablissement_id)
        SELECT
            source.source_etablissement_id,
            target.etablissement_id
        FROM sourcewithdependences AS source
        LEFT JOIN public.etablissements AS target
            ON (target.entreprise_id = source.grandparent)
            AND (source.etablissement_key = target.etablissement_key);
    

        CALL log.log_script('monthly_integration', 'load_etablissements', 'END');
            