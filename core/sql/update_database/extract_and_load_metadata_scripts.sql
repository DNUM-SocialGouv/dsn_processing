

        CALL log.log_script('update_database', 'extract_and_load_metadata_scripts', 'BEGIN');
            


        TRUNCATE TABLE sys.metadata_scripts CASCADE;
        COPY sys.metadata_scripts
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'UTF8'
        );
    

        CALL log.log_script('update_database', 'extract_and_load_metadata_scripts', 'END');
            