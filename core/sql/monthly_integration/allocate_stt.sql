

        CALL log.log_script('monthly_integration', 'allocate_stt', 'BEGIN');
            

        -- stt means "salarié de travail temporaire"
        WITH stt AS (
            SELECT
                c.etu_id AS etablissement_id,
                s.salarie_key,
                s.nir,
                s.nom_famille,
                s.nom_usage,
                s.prenoms,
                s.date_naissance,
                s.lieu_naissance,
                TRUE AS allocated_from_ett,
                s.date_derniere_declaration
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON s.salarie_id = c.salarie_id
            INNER JOIN public.etablissements AS e
                ON e.etablissement_id = c.etablissement_id
            WHERE e.code_naf = '7820Z' AND c.code_nature_contrat = '03' AND c.etu_id IS NOT NULL
        ),

        -- remove duplicates (duplicates are possible since there is no uniqueness constraint on the tuple (etu_id, salarie_key))
        sttwithoutduplicates AS (
            SELECT
                etablissement_id,
                salarie_key,
                nir,
                nom_famille,
                nom_usage,
                prenoms,
                date_naissance,
                lieu_naissance,
                allocated_from_ett,
                date_derniere_declaration
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY etablissement_id, salarie_key ORDER BY 1) AS row_num
                FROM stt) AS ord
            WHERE row_num = 1
        )

        -- insert stt which do not already exist in etu
        INSERT INTO public.salaries (
            etablissement_id,
            salarie_key,
            nir,
            nom_famille,
            nom_usage,
            prenoms,
            date_naissance,
            lieu_naissance,
            allocated_from_ett,
            date_derniere_declaration
        )
        SELECT *
        FROM sttwithoutduplicates
        ON CONFLICT ON CONSTRAINT uk_salarie DO NOTHING;
        

        CALL log.log_script('monthly_integration', 'allocate_stt', 'END');
            