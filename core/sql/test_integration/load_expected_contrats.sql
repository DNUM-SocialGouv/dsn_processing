

        CALL log.log_script('test_integration', 'load_expected_contrats', 'BEGIN');
            

        TRUNCATE TABLE test.expected_contrats CASCADE;
        COPY test.expected_contrats
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_contrats', 'END');
            