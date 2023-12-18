

        CALL log.log_script('monthly_integration', 'modify_ouverture_etablissements', 'BEGIN');
            

        -- Fermeture des etablissements n'ayant pas fait l'objet de déclarations pendant 3 mois
        UPDATE public.etablissements
        SET ouvert = FALSE
        FROM public.chargement_donnees
        WHERE public.chargement_donnees.dernier_mois_de_declaration_integre >= (public.etablissements.date_derniere_declaration + INTERVAL '3 MONTH');

        -- Ré-ouverture des établissements ayant une déclaration de moins de 3 mois
        UPDATE public.etablissements
        SET ouvert = TRUE
        FROM public.chargement_donnees
        WHERE public.chargement_donnees.dernier_mois_de_declaration_integre < (public.etablissements.date_derniere_declaration + INTERVAL '3 MONTH');
        

        CALL log.log_script('monthly_integration', 'modify_ouverture_etablissements', 'END');
            