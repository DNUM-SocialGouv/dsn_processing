

        CALL log.log_script('update_database', 'update_zonage', 'BEGIN');
            


        TRUNCATE TABLE raw.raw_zonage CASCADE;
        COPY raw.raw_zonage
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        TRUNCATE TABLE public.zonage;
        INSERT INTO public.zonage(
            siret,
            unite_controle
        )
        SELECT
            CAST(TRIM(siret) AS BIGINT) AS siret,
            CAST(unite_controle AS CHAR(6)) AS unite_controle
        FROM raw.raw_zonage;
            

        CALL log.log_script('update_database', 'update_zonage', 'END');
            