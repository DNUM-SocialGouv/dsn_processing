

        CALL log.log_script('update_database', 'update_calendar', 'BEGIN');
            

        WITH generate_calendar AS (
            SELECT GENERATE_SERIES(GREATEST('2000-01-01', MAX(full_date) + '1 day'::INTERVAL), GREATEST('2030-01-01', MAKE_DATE(EXTRACT(YEAR FROM NOW())::INT + 5, 1, 1)), '1 day'::INTERVAL)::DATE AS full_date
            FROM public.daily_calendar
        )

        INSERT INTO public.daily_calendar
        SELECT
            full_date,
            EXTRACT(DAY FROM full_date) AS day_number,
            EXTRACT(MONTH FROM full_date) AS month_number,
            EXTRACT(YEAR FROM full_date) AS year_number,
            EXTRACT(DOW FROM full_date) AS weekday_number
        FROM generate_calendar;
        

        CALL log.log_script('update_database', 'update_calendar', 'END');
            