

        CALL log.log_script('monthly_integration', 'remove_ctt', 'BEGIN');
            

        -- delete allocated contrats in the etu
        DELETE FROM public.contrats
        WHERE public.contrats.ett_contrat_id IS NOT NULL;
        

        CALL log.log_script('monthly_integration', 'remove_ctt', 'END');
            