

        CALL log.log_script('monthly_integration', 'load_contrats', 'BEGIN');
            

        -- On importe les données avec les id "généalogiques" des tables parent et grandparent permanentes
        WITH sourcewithdependences AS (
            SELECT
                source.source_contrats.*,
                link_grandparent.etablissement_id AS grandparent,
                link_parent.salarie_id AS parent
            FROM source.source_contrats
            LEFT JOIN source.link_etablissements AS link_grandparent
                ON link_grandparent.source_etablissement_id = source.source_contrats.source_etablissement_id
            LEFT JOIN source.link_salaries AS link_parent
                ON link_parent.source_salarie_id = source.source_contrats.source_salarie_id
        ),

        -- On supprime les doublons des données
        sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY grandparent, parent, contrat_key ORDER BY date_derniere_declaration DESC) AS row_num
                FROM sourcewithdependences
            ) AS ord
            WHERE row_num = 1
        )

        -- On merge les données entrantes dans la table permanente sur la base de la key_column et des id des tables parent et grandparent
        MERGE INTO public.contrats AS target
        USING sourcewithoutduplicates AS source
            ON (target.etablissement_id = source.grandparent)
            AND (target.salarie_id = source.parent)
            AND (target.contrat_key = source.contrat_key)
        -- Si la donnée n'est pas présente dans la table, on l'insert
        WHEN NOT MATCHED THEN
            INSERT (etablissement_id,
                    salarie_id,
                    contrat_key,
                    date_debut,numero,poste_id,code_nature_contrat,code_convention_collective,code_motif_recours,code_dispositif_public,code_complement_dispositif_public,lieu_travail,code_mise_disposition_externe,date_fin_previsionnelle,etu_id,date_derniere_declaration
                    )
            VALUES (source.grandparent,
                    source.parent,
                    source.contrat_key,
                     source.date_debut, source.numero, source.poste_id, source.code_nature_contrat, source.code_convention_collective, source.code_motif_recours, source.code_dispositif_public, source.code_complement_dispositif_public, source.lieu_travail, source.code_mise_disposition_externe, source.date_fin_previsionnelle, source.etu_id, source.date_derniere_declaration
                    )
        -- Sinon, on update les informations auxiliaires
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            UPDATE SET contrat_key = source.contrat_key, date_debut = source.date_debut, numero = source.numero, poste_id = source.poste_id, code_nature_contrat = source.code_nature_contrat, code_convention_collective = source.code_convention_collective, code_motif_recours = source.code_motif_recours, code_dispositif_public = source.code_dispositif_public, code_complement_dispositif_public = source.code_complement_dispositif_public, lieu_travail = source.lieu_travail, code_mise_disposition_externe = source.code_mise_disposition_externe, date_fin_previsionnelle = source.date_fin_previsionnelle, etu_id = source.etu_id, date_derniere_declaration = source.date_derniere_declaration;

        -- A nouveau, on considère les données entrantes avec les id "généalogiques" des tables grandparent et parent permanente
        TRUNCATE TABLE source.link_contrats CASCADE;
        WITH sourcewithdependences AS (
            SELECT
                source.source_contrats.source_contrat_id,
                source.source_contrats.contrat_key,
                link.etablissement_id AS grandparent,
                link_parent.salarie_id AS parent
            FROM source.source_contrats
            LEFT JOIN source.link_etablissements AS link
                ON link.source_etablissement_id = source.source_contrats.source_etablissement_id
            LEFT JOIN source.link_salaries AS link_parent
                ON link_parent.source_salarie_id = source.source_contrats.source_salarie_id
        )

        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        INSERT INTO source.link_contrats (source_contrat_id, contrat_id)
        SELECT
            source.source_contrat_id,
            target.contrat_id
        FROM sourcewithdependences AS source
        LEFT JOIN public.contrats AS target
            ON (target.etablissement_id = source.grandparent)
            AND (target.salarie_id = source.parent)
            AND (source.contrat_key = target.contrat_key);
    

        CALL log.log_script('monthly_integration', 'load_contrats', 'END');
            