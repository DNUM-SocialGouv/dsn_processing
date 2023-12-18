

        CALL log.log_script('mock_integration', 'load_mock_contrats', 'BEGIN');
            

        TRUNCATE TABLE public.contrats CASCADE;
        COPY public.contrats
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_contrats', 'END');
            