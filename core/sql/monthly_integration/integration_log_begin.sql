

        INSERT INTO log.integrations_logs(declaration_date, stage_type, stage_time) VALUES ('{{ params.filedate }}', 'BEGIN', CLOCK_TIMESTAMP());
        