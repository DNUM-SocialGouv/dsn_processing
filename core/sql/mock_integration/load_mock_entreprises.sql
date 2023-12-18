

        CALL log.log_script('mock_integration', 'load_mock_entreprises', 'BEGIN');
            

        TRUNCATE TABLE public.entreprises CASCADE;
        COPY public.entreprises
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_entreprises', 'END');
            