

        CALL log.log_script('monthly_integration', 'remove_old_data', 'BEGIN');
            


        DELETE FROM public.entreprises AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= 72;
    

        DELETE FROM public.etablissements AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= 72;
    

        DELETE FROM public.salaries AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= 72;
    

        DELETE FROM public.contrats AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= 72;
    

        DELETE FROM public.postes AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= 72;
    

        CALL log.log_script('monthly_integration', 'remove_old_data', 'END');
            