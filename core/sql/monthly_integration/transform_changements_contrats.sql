

        CALL log.log_script('monthly_integration', 'transform_changements_contrats', 'BEGIN');
            

        TRUNCATE TABLE source.source_changements_contrats CASCADE;
        WITH round1 AS (
            SELECT
                idcontratchangement,
                idcontrat,
                datemodification,
                siretetab,
                numero,
                datedebut,
                datedeclaration
            FROM raw.raw_changements_contrats
        ),

        round2 AS (
            SELECT
                current_.idcontratchangement,
                current_.idcontrat,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.siretetab, next_.siretetab) AS siretetab,
                COALESCE(current_.numero, next_.numero) AS numero,
                COALESCE(current_.datedebut, next_.datedebut) AS datedebut,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round1 AS current_
            LEFT JOIN raw.raw_changements_contrats AS next_
                ON current_.idcontrat = next_.idcontrat
                AND (
                    ((current_.siretetab IS NULL OR next_.siretetab IS NULL)
                        AND (current_.numero IS NULL OR next_.numero IS NULL)
                        AND (current_.datedebut IS NULL OR next_.datedebut IS NULL))
                    OR current_.idcontratchangement = next_.idcontratchangement
                )
        ),

        round3 AS (
            SELECT
                current_.idcontrat,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.siretetab, next_.siretetab) AS siretetab,
                COALESCE(current_.numero, next_.numero) AS numero,
                COALESCE(current_.datedebut, next_.datedebut) AS datedebut,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round2 AS current_
            LEFT JOIN raw.raw_changements_contrats AS next_
                ON current_.idcontrat = next_.idcontrat
                AND (
                    ((current_.siretetab IS NULL OR next_.siretetab IS NULL)
                        AND (current_.numero IS NULL OR next_.numero IS NULL)
                        AND (current_.datedebut IS NULL OR next_.datedebut IS NULL))
                    OR current_.idcontratchangement = next_.idcontratchangement
                )
        )

        INSERT INTO source.source_changements_contrats (
            source_contrat_id,
            date_modification,
            siret_ancien_employeur,
            ancien_numero,
            ancienne_date_debut,
            date_derniere_declaration
        )
        SELECT DISTINCT
            idcontrat AS source_contrat_id,
            datemodification AS date_modification,
            CAST(siretetab AS BIGINT) AS siret_ancien_employeur,
            UPPER(TRIM(numero)) AS ancien_numero,
            datedebut AS ancienne_date_debut,
            datedeclaration AS date_derniere_declaration
        FROM round3
        WHERE siretetab IS NOT NULL
            OR numero IS NOT NULL
            OR datedebut IS NOT NULL;
        

        CALL log.log_script('monthly_integration', 'transform_changements_contrats', 'END');
            