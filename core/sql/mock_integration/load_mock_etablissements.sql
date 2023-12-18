

        CALL log.log_script('mock_integration', 'load_mock_etablissements', 'BEGIN');
            

        TRUNCATE TABLE public.etablissements CASCADE;
        COPY public.etablissements
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_etablissements', 'END');
            