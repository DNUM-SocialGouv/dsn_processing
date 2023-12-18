

        CALL log.log_script('update_database', 'update_holidays', 'BEGIN');
            


        DROP TABLE IF EXISTS public.holidays_calendar CASCADE;
        CREATE TABLE public.holidays_calendar (
            holiday_date DATE NOT NULL
        );
            

        TRUNCATE TABLE public.holidays_calendar CASCADE;
        COPY public.holidays_calendar
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'WIN1252'
        );
    

        UPDATE public.daily_calendar
        SET public_holiday = COALESCE((EXISTS (SELECT 1 FROM public.holidays_calendar WHERE holiday_date = full_date) OR public_holiday IS TRUE), FALSE);
            

        DROP TABLE public.holidays_calendar;
            

        CALL log.log_script('update_database', 'update_holidays', 'END');
            