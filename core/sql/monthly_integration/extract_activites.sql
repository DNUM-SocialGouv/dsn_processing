

        CALL log.log_script('monthly_integration', 'extract_activites', 'BEGIN');
            

        TRUNCATE TABLE raw.raw_activites CASCADE;
        COPY raw.raw_activites
        FROM '{{ params.filepath }}/{{ params.filetype }}/{{ params.foldername }}_{{ params.filedate }}/{{ params.filename }}_{{ params.filedate }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('monthly_integration', 'extract_activites', 'END');
            