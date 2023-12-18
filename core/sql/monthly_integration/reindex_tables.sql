

        CALL log.log_script('monthly_integration', 'reindex_tables', 'BEGIN');
            

        REINDEX TABLE contrats;
        REINDEX TABLE salaries;
        

        CALL log.log_script('monthly_integration', 'reindex_tables', 'END');
            