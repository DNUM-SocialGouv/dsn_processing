

        CALL log.log_script('update_database', 'update_motifs_recours', 'BEGIN');
            


        CREATE TEMPORARY TABLE update_motifs_recours (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    

        TRUNCATE TABLE update_motifs_recours CASCADE;
        COPY update_motifs_recours
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        MERGE INTO public.motifs_recours
        USING update_motifs_recours
            ON public.motifs_recours.code_motif_recours = update_motifs_recours.code
        WHEN MATCHED THEN
            UPDATE SET libelle = update_motifs_recours.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                code_motif_recours,
                libelle
            ) VALUES (
                update_motifs_recours.code,
                update_motifs_recours.libelle
            );
    

        DROP TABLE update_motifs_recours;
        

        CALL log.log_script('update_database', 'update_motifs_recours', 'END');
            