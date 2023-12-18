


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
    