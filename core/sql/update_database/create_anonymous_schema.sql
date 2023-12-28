

        CALL log.log_script('update_database', 'create_anonymous_schema', 'BEGIN');
            


        DROP SCHEMA IF EXISTS anonymous CASCADE;
        CREATE SCHEMA anonymous;

        CREATE TABLE anonymous.selection_etablissements (
            etablissement_key BIGINT NOT NULL,
            comment VARCHAR(100)
        );
        

        TRUNCATE TABLE anonymous.selection_etablissements CASCADE;
        COPY anonymous.selection_etablissements
        FROM '{{ params.filepath }}/{{ params.filename }}.csv'
        WITH (
            FORMAT 'csv',
            HEADER,
            FREEZE,
            DELIMITER ';',
            QUOTE '|',
            ENCODING 'UTF8'
        );
    

        ALTER TABLE anonymous.selection_etablissements
        ADD COLUMN etablissement_id BIGINT;

        UPDATE anonymous.selection_etablissements
        SET etablissement_id = etablissements.etablissement_id
        FROM public.etablissements 
        WHERE selection_etablissements.etablissement_key = etablissements.etablissement_key;

        INSERT INTO anonymous.selection_etablissements (
            etablissement_key,
            comment,
            etablissement_id
        )

        SELECT DISTINCT
            ett.etablissement_key,
            'ETT référencé' AS reason,
            ett.etablissement_id
        FROM public.contrats
        INNER JOIN anonymous.selection_etablissements
            ON contrats.etablissement_id = selection_etablissements.etablissement_id
        INNER JOIN public.contrats AS ctt
            ON contrats.ett_contrat_id IS NOT NULL AND contrats.ett_contrat_id = ctt.contrat_id
        INNER JOIN public.etablissements AS ett
            ON ctt.etablissement_id = ett.etablissement_id;
        

        CREATE TABLE anonymous.chargement_donnees AS TABLE public.chargement_donnees;
        CREATE TABLE anonymous.categories_juridiques_insee AS TABLE public.categories_juridiques_insee;
        CREATE TABLE anonymous.conventions_collectives AS TABLE public.conventions_collectives;
        CREATE TABLE anonymous.motifs_recours AS TABLE public.motifs_recours;
        CREATE TABLE anonymous.natures_contrats AS TABLE public.natures_contrats;
        CREATE TABLE anonymous.naf AS TABLE public.naf;
        -- CREATE TABLE anonymous.zonage AS TABLE public.zonage;
        CREATE TABLE anonymous.daily_calendar AS TABLE public.daily_calendar;
        

        CREATE TABLE anonymous.etablissements AS

        WITH upper_bound AS (
            SELECT MAX(etablissement_id) AS max_id
            FROM public.etablissements
        ),

        unnamed_data AS (
            SELECT
                upper_bound.max_id - etablissements.etablissement_id + 1 AS twin_id,
                etablissements.*
            FROM public.etablissements, upper_bound
            WHERE etablissements.etablissement_id IN (SELECT etablissement_id FROM anonymous.selection_etablissements)
        ),

        renamed_data AS (
            SELECT 
                unnamed_data.etablissement_id,
                unnamed_data.entreprise_id,
                twin_data.etablissement_key,
                unnamed_data.siege_social,
                unnamed_data.ouvert,
                twin_data.enseigne,
                twin_data.adresse,
                twin_data.complement_adresse,
                unnamed_data.code_postal,
                unnamed_data.commune,
                unnamed_data.code_categorie_juridique_insee,
                unnamed_data.code_naf,
                unnamed_data.code_convention_collective,
                unnamed_data.date_premiere_declaration,
                unnamed_data.date_derniere_declaration
            FROM unnamed_data
            INNER JOIN public.etablissements AS twin_data
                ON twin_data.etablissement_id = unnamed_data.twin_id
        )

        SELECT * FROM renamed_data;
    

        CREATE TABLE anonymous.entreprises AS

        WITH upper_bound AS (
            SELECT MAX(entreprise_id) AS max_id
            FROM public.entreprises
        ),

        unnamed_data AS (
            SELECT
                upper_bound.max_id - entreprises.entreprise_id + 1 AS twin_id,
                entreprises.*
            FROM public.entreprises, upper_bound
            WHERE entreprises.entreprise_id IN (SELECT entreprise_id FROM anonymous.etablissements)
        ),

        renamed_data AS (
            SELECT 
                unnamed_data.entreprise_id,
                twin_data.entreprise_key,
                twin_data.raison_sociale,
                unnamed_data.date_premiere_declaration,
                unnamed_data.date_derniere_declaration
            FROM unnamed_data
            INNER JOIN public.entreprises AS twin_data
                ON twin_data.entreprise_id = unnamed_data.twin_id
        )

        SELECT * FROM renamed_data;
    

        CREATE TABLE anonymous.salaries AS

        WITH upper_bound AS (
            SELECT MAX(salarie_id) AS max_id
            FROM public.salaries
        ),

        unnamed_data AS (
            SELECT
                upper_bound.max_id - salaries.salarie_id + 1 AS twin_id,
                salaries.*
            FROM public.salaries, upper_bound
            WHERE salaries.etablissement_id IN (SELECT etablissement_id FROM anonymous.etablissements) AND salaries.nir IS NOT NULL
        ),

        renamed_data AS (
            SELECT 
                unnamed_data.salarie_id,
                unnamed_data.etablissement_id,
                unnamed_data.salarie_key,
                unnamed_data.nir,
                twin_data.nom_famille,
                twin_data.nom_usage,
                twin_data.prenoms,
                twin_data.date_naissance,
                twin_data.lieu_naissance,
                unnamed_data.allocated_from_ett,
                unnamed_data.date_derniere_declaration
            FROM unnamed_data
            INNER JOIN public.salaries AS twin_data
                ON twin_data.salarie_id = unnamed_data.twin_id
        )

        SELECT * FROM renamed_data;
    

        CREATE TABLE anonymous.contrats AS
        SELECT contrats.*
        FROM public.contrats
        INNER JOIN anonymous.etablissements
            ON etablissements.etablissement_id = contrats.etablissement_id;
        

        CREATE TABLE anonymous.postes AS
        SELECT DISTINCT postes.*
        FROM public.postes
        INNER JOIN anonymous.contrats
            ON contrats.poste_id = postes.poste_id;
        

        CREATE TABLE anonymous.activites AS
        SELECT activites.*
        FROM public.activites
        INNER JOIN anonymous.contrats
            ON COALESCE(contrats.ett_contrat_id, contrats.contrat_id) = activites.contrat_id;
        

        CALL log.log_script('update_database', 'create_anonymous_schema', 'END');
            