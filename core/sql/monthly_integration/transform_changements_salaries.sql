

        CALL log.log_script('monthly_integration', 'transform_changements_salaries', 'BEGIN');
            

        TRUNCATE TABLE source.source_changements_salaries CASCADE;
        WITH round1 AS (
            SELECT
                idindividuchangement,
                idindividu,
                datemodification,
                anciennir,
                nomfamille,
                prenoms,
                datenaissance,
                datedeclaration
            FROM raw.raw_changements_salaries
        ),

        round2 AS (
            SELECT
                current_.idindividuchangement,
                current_.idindividu,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.anciennir, next_.anciennir) AS anciennir,
                COALESCE(current_.nomfamille, next_.nomfamille) AS nomfamille,
                COALESCE(current_.prenoms, next_.prenoms) AS prenoms,
                COALESCE(current_.datenaissance, next_.datenaissance) AS datenaissance,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round1 AS current_
            LEFT JOIN raw.raw_changements_salaries AS next_
                ON current_.idindividu = next_.idindividu
                AND COALESCE(current_.anciennir, 'NULL') = COALESCE(next_.anciennir, 'NULL')
                AND (
                    ((current_.nomfamille IS NULL OR next_.nomfamille IS NULL)
                        AND (current_.prenoms IS NULL OR next_.prenoms IS NULL)
                        AND (current_.datenaissance IS NULL OR next_.datenaissance IS NULL))
                    OR current_.idindividuchangement = next_.idindividuchangement
                )
        ),

        round3 AS (
            SELECT
                current_.idindividu,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.anciennir, next_.anciennir) AS anciennir,
                COALESCE(current_.nomfamille, next_.nomfamille) AS nomfamille,
                COALESCE(current_.prenoms, next_.prenoms) AS prenoms,
                COALESCE(current_.datenaissance, next_.datenaissance) AS datenaissance,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round2 AS current_
            LEFT JOIN raw.raw_changements_salaries AS next_
                ON current_.idindividu = next_.idindividu
                AND COALESCE(current_.anciennir, 'NULL') = COALESCE(next_.anciennir, 'NULL')
                AND (
                    ((current_.nomfamille IS NULL OR next_.nomfamille IS NULL)
                        AND (current_.prenoms IS NULL OR next_.prenoms IS NULL)
                        AND (current_.datenaissance IS NULL OR next_.datenaissance IS NULL))
                    OR current_.idindividuchangement = next_.idindividuchangement
                )
        )

        INSERT INTO source.source_changements_salaries (
            source_salarie_id,
            date_modification,
            ancien_nir,
            ancien_nom_famille,
            anciens_prenoms,
            ancienne_date_naissance,
            date_derniere_declaration
        )
        SELECT DISTINCT
            idindividu AS source_salarie_id,
            datemodification AS date_modification,
            CAST(anciennir AS BIGINT) AS ancien_nir,
            UPPER(TRIM(nomfamille)) AS ancien_nom_famille,
            UPPER(TRIM(prenoms)) AS anciens_prenoms,
            OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0) AS ancienne_date_naissance,
            datedeclaration AS date_derniere_declaration
        FROM round3
        WHERE anciennir IS NOT NULL
            OR nomfamille IS NOT NULL
            OR prenoms IS NOT NULL
            OR datenaissance IS NOT NULL;
        

        CALL log.log_script('monthly_integration', 'transform_changements_salaries', 'END');
            