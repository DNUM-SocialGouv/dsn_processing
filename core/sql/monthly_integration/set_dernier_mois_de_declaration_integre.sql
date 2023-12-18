

        CALL log.log_script('monthly_integration', 'set_dernier_mois_de_declaration_integre', 'BEGIN');
            

        UPDATE public.chargement_donnees
        SET dernier_mois_de_declaration_integre = '{{ params.filedate }}';
        

        CALL log.log_script('monthly_integration', 'set_dernier_mois_de_declaration_integre', 'END');
            