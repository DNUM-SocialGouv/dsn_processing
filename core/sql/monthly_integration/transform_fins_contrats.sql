

        CALL log.log_script('monthly_integration', 'transform_fins_contrats', 'BEGIN');
            

        TRUNCATE TABLE source.source_fins_contrats CASCADE;
        INSERT INTO source.source_fins_contrats (
            source_contrat_fin_id,
            source_contrat_id,
            date_fin,
            code_motif_rupture,
            date_derniere_declaration
        ) SELECT
            idcontratfin AS source_contrat_fin_id,
            idcontrat AS source_contrat_id,
            datefin AS date_fin,
            codemotifrupture AS code_motif_rupture,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_fins_contrats;
        

        CALL log.log_script('monthly_integration', 'transform_fins_contrats', 'END');
            