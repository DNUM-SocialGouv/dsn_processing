

        CALL log.log_script('monthly_integration', 'remove_stt', 'BEGIN');
            

        -- delete allocated salaries in the etu
        DELETE FROM public.salaries
        WHERE public.salaries.allocated_from_ett IS TRUE;
        

        CALL log.log_script('monthly_integration', 'remove_stt', 'END');
            