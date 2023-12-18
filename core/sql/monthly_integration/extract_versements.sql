

        CALL log.log_script('monthly_integration', 'extract_versements', 'BEGIN');
            

        TRUNCATE TABLE raw.raw_versements CASCADE;
        COPY raw.raw_versements
        FROM '{{ params.filepath }}/{{ params.filetype }}/{{ params.foldername }}_{{ params.filedate }}/{{ params.filename }}_{{ params.filedate }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('monthly_integration', 'extract_versements', 'END');
            