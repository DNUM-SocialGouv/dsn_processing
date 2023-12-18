

        CALL log.log_script('test_integration', 'create_expected_tables', 'BEGIN');
            

        DROP SCHEMA IF EXISTS test CASCADE;
        CREATE SCHEMA IF NOT EXISTS test;

        DROP TABLE IF EXISTS test.expected_entreprises CASCADE;
        CREATE TABLE test.expected_entreprises AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.entreprises
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_etablissements CASCADE;
        CREATE TABLE test.expected_etablissements AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.etablissements
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_salaries CASCADE;
        CREATE TABLE test.expected_salaries AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.salaries
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_contrats CASCADE;
        CREATE TABLE test.expected_contrats AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.contrats
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_postes CASCADE;
        CREATE TABLE test.expected_postes AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.postes
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_activites CASCADE;
        CREATE TABLE test.expected_activites AS (
            SELECT
                NULL::INT AS sujet_id,
                *
            FROM public.activites
            LIMIT 0
        );

        DROP TABLE IF EXISTS test.expected_contrats_comparisons CASCADE;
        CREATE TABLE test.expected_contrats_comparisons (
            contrat_id BIGINT PRIMARY KEY NOT NULL,
            date_fin_effective_comparison SMALLINT
        );
        

        CALL log.log_script('test_integration', 'create_expected_tables', 'END');
            