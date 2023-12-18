

        CALL log.log_script('test_integration', 'load_expected_salaries', 'BEGIN');
            

        TRUNCATE TABLE test.expected_salaries CASCADE;
        COPY test.expected_salaries
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_salaries', 'END');
            