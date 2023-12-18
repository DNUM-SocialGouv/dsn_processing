

        CALL log.log_script('test_integration', 'load_expected_entreprises', 'BEGIN');
            

        TRUNCATE TABLE test.expected_entreprises CASCADE;
        COPY test.expected_entreprises
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_entreprises', 'END');
            