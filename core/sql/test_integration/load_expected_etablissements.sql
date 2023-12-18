

        CALL log.log_script('test_integration', 'load_expected_etablissements', 'BEGIN');
            

        TRUNCATE TABLE test.expected_etablissements CASCADE;
        COPY test.expected_etablissements
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('test_integration', 'load_expected_etablissements', 'END');
            