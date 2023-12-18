

        CALL log.log_script('mock_integration', 'load_mock_activites', 'BEGIN');
            

        TRUNCATE TABLE public.activites CASCADE;
        COPY public.activites
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_activites', 'END');
            