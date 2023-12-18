

        CALL log.log_script('test_integration', 'load_expected_postes', 'BEGIN');
            

        TRUNCATE TABLE test.expected_postes CASCADE;
        COPY test.expected_postes
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_postes', 'END');
            