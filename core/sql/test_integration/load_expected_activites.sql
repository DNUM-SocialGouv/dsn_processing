

        CALL log.log_script('test_integration', 'load_expected_activites', 'BEGIN');
            

        TRUNCATE TABLE test.expected_activites CASCADE;
        COPY test.expected_activites
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_activites', 'END');
            