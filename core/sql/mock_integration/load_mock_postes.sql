

        CALL log.log_script('mock_integration', 'load_mock_postes', 'BEGIN');
            

        TRUNCATE TABLE public.postes CASCADE;
        COPY public.postes
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_postes', 'END');
            