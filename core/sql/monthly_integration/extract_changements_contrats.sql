

        CALL log.log_script('monthly_integration', 'extract_changements_contrats', 'BEGIN');
            

        TRUNCATE TABLE raw.raw_changements_contrats CASCADE;
        COPY raw.raw_changements_contrats
        FROM '{{ params.filepath }}/{{ params.filetype }}/{{ params.foldername }}_{{ params.filedate }}/{{ params.filename }}_{{ params.filedate }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        CALL log.log_script('monthly_integration', 'extract_changements_contrats', 'END');
            