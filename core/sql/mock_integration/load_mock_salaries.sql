

        CALL log.log_script('mock_integration', 'load_mock_salaries', 'BEGIN');
            

        TRUNCATE TABLE public.salaries CASCADE;
        COPY public.salaries
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('mock_integration', 'load_mock_salaries', 'END');
            