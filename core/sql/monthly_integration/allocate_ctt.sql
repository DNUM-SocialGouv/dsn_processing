

        CALL log.log_script('monthly_integration', 'allocate_ctt', 'BEGIN');
            

        -- ctt means "contrat de travail temporaire"
        WITH ctt AS (
            SELECT
                c.etu_id AS etablissement_id,
                s.salarie_key,
                CONCAT(c.numero, '_', e.etablissement_key, '_', CAST(c.date_debut AS CHAR(10))) AS contrat_key,
                CONCAT(c.numero, '_', e.etablissement_key) AS numero,
                c.date_debut,
                c.poste_id,
                c.code_nature_contrat,
                c.code_convention_collective,
                c.code_motif_recours,
                c.code_dispositif_public,
                c.code_complement_dispositif_public,
                c.lieu_travail,
                c.code_mise_disposition_externe,
                c.date_fin_previsionnelle,
                c.date_fin_effective,
                c.statut_fin,
                c.contrat_id AS ett_contrat_id,
                c.date_derniere_declaration
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON s.salarie_id = c.salarie_id
            INNER JOIN public.etablissements AS e
                ON e.etablissement_id = c.etablissement_id
            WHERE e.code_naf = '7820Z' AND c.code_nature_contrat = '03' AND c.etu_id IS NOT NULL
        )

        -- insert ctt in contrats (knowing that stt have already been pushed in salaries)
        INSERT INTO public.contrats (
            etablissement_id,
            salarie_id,
            contrat_key,
            numero,
            date_debut,
            poste_id,
            code_nature_contrat,
            code_convention_collective,
            code_motif_recours,
            code_dispositif_public,
            code_complement_dispositif_public,
            lieu_travail,
            code_mise_disposition_externe,
            date_fin_previsionnelle,
            date_fin_effective,
            statut_fin,
            ett_contrat_id,
            date_derniere_declaration)

        (
            SELECT
                ctt.etablissement_id,
                s.salarie_id,
                ctt.contrat_key,
                ctt.numero,
                ctt.date_debut,
                ctt.poste_id,
                ctt.code_nature_contrat,
                ctt.code_convention_collective,
                ctt.code_motif_recours,
                ctt.code_dispositif_public,
                ctt.code_complement_dispositif_public,
                ctt.lieu_travail,
                ctt.code_mise_disposition_externe,
                ctt.date_fin_previsionnelle,
                ctt.date_fin_effective,
                ctt.statut_fin,
                ctt.ett_contrat_id,
                ctt.date_derniere_declaration
            FROM ctt
            INNER JOIN public.salaries AS s
                ON ctt.etablissement_id = s.etablissement_id
                AND ctt.salarie_key = s.salarie_key)
        ON CONFLICT ON CONSTRAINT uk_contrat DO NOTHING;
        

        CALL log.log_script('monthly_integration', 'allocate_ctt', 'END');
            