

        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA IF NOT EXISTS public;

        DROP SCHEMA IF EXISTS log CASCADE;
        CREATE SCHEMA IF NOT EXISTS log;

        DROP SCHEMA IF EXISTS raw CASCADE;
        CREATE SCHEMA IF NOT EXISTS raw;

        DROP SCHEMA IF EXISTS source CASCADE;
        CREATE SCHEMA IF NOT EXISTS source;

        DROP SCHEMA IF EXISTS sys CASCADE;
        CREATE SCHEMA IF NOT EXISTS sys;

        DROP TABLE IF EXISTS public.entreprises CASCADE;
        CREATE TABLE public.entreprises (
            entreprise_id BIGSERIAL PRIMARY KEY NOT NULL,
            entreprise_key BIGINT NOT NULL,
            raison_sociale VARCHAR(80),
            date_premiere_declaration DATE NOT NULL,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_entreprise UNIQUE (entreprise_key)
        );

        -- VARCHAR length specifications are infered from the "cahier technique de la DSN"
        DROP TABLE IF EXISTS public.etablissements CASCADE;
        CREATE TABLE public.etablissements (
            etablissement_id BIGSERIAL PRIMARY KEY NOT NULL,
            entreprise_id BIGINT NOT NULL,
            etablissement_key BIGINT NOT NULL,
            siege_social BOOLEAN NOT NULL,
            ouvert BOOLEAN NOT NULL DEFAULT TRUE,
            enseigne VARCHAR(80),
            adresse VARCHAR(50),
            complement_adresse VARCHAR(50),
            code_postal CHAR(5),
            commune VARCHAR(50),
            code_categorie_juridique_insee CHAR(4),
            code_naf CHAR(5) NOT NULL,
            code_convention_collective CHAR(4), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            date_premiere_declaration DATE NOT NULL,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_etablissement UNIQUE (etablissement_key)
        );

        DROP TABLE IF EXISTS public.salaries CASCADE;
        CREATE TABLE public.salaries (
            salarie_id BIGSERIAL PRIMARY KEY NOT NULL,
            etablissement_id BIGINT NOT NULL,
            salarie_key VARCHAR(170) NOT NULL,
            nir BIGINT,
            nom_famille VARCHAR(80) NOT NULL,
            nom_usage VARCHAR(80),
            prenoms VARCHAR(80) NOT NULL,
            date_naissance CHAR(10) NOT NULL,
            lieu_naissance VARCHAR(30),
            allocated_from_ett BOOLEAN,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_salarie UNIQUE (etablissement_id, salarie_key),
            CONSTRAINT vk_salarie CHECK (COALESCE(CAST(nir AS CHAR(13)), CONCAT(nom_famille, prenoms, date_naissance)) = salarie_key)
        );

        DROP TABLE IF EXISTS public.contrats CASCADE;
        CREATE TABLE public.contrats (
            contrat_id BIGSERIAL PRIMARY KEY NOT NULL,
            etablissement_id BIGINT NOT NULL,
            salarie_id BIGINT NOT NULL,
            contrat_key VARCHAR(46) NOT NULL, -- VARCHAR(46) = VARCHAR(35) from numero + VARCHAR(1) for "_" + VARCHAR(10) from date_debut
            numero VARCHAR(35) NOT NULL, -- 20 + 15 (numero should be VARCHAR(20), we increase it to VARCHAR(35) in order to be able to put _siret for specific cases)
            date_debut DATE NOT NULL,
            date_debut_effective DATE, -- if another date_debut can be infered (e.g employer's transfer) else NULL
            poste_id BIGINT NOT NULL,
            code_nature_contrat CHAR(2) NOT NULL,
            code_convention_collective CHAR(4) NOT NULL,
            code_motif_recours CHAR(2),
            code_dispositif_public CHAR(2),
            code_complement_dispositif_public CHAR(2),
            lieu_travail VARCHAR(14),
            code_mise_disposition_externe CHAR(2),
            date_fin_previsionnelle DATE,
            date_fin_effective DATE,
            statut_fin SMALLINT,
            etu_id BIGINT,
            ett_contrat_id BIGINT,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_contrat UNIQUE (etablissement_id, salarie_id, contrat_key),
            CONSTRAINT vk_contrat CHECK (contrat_key = CONCAT(numero, '_', CAST(date_debut AS CHAR(10)))),
            CONSTRAINT consistent_date_fin_effective CHECK ((statut_fin IS NULL AND date_fin_effective IS NULL) OR (statut_fin IN (1, 2) AND date_fin_effective IS NOT NULL))
        );

        DROP TABLE IF EXISTS public.postes CASCADE;
        CREATE TABLE public.postes (
            poste_id BIGSERIAL PRIMARY KEY NOT NULL,
            poste_key VARCHAR(120) NOT NULL,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_poste UNIQUE (poste_key)
        );


        DROP TABLE IF EXISTS public.activites;
        CREATE TABLE public.activites (
            activite_id BIGSERIAL PRIMARY KEY NOT NULL,
            contrat_id BIGINT NOT NULL,
            mois DATE NOT NULL,
            heures_standards_remunerees REAL NOT NULL,
            heures_non_remunerees REAL NOT NULL,
            heures_supp_remunerees REAL NOT NULL,
            date_derniere_declaration DATE NOT NULL,
            CONSTRAINT uk_activites UNIQUE (contrat_id, mois)
        );

        DROP TABLE IF EXISTS public.chargement_donnees CASCADE;
        CREATE TABLE public.chargement_donnees (
            dernier_mois_de_declaration_integre DATE
        );

        DROP TABLE IF EXISTS public.natures_contrats CASCADE;
        CREATE TABLE public.natures_contrats (
            code_nature_contrat CHAR(2) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );

        DROP TABLE IF EXISTS public.motifs_recours CASCADE;
        CREATE TABLE public.motifs_recours (
            code_motif_recours CHAR(2) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );

        DROP TABLE IF EXISTS public.naf CASCADE;
        CREATE TABLE public.naf (
            code_naf CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );

        DROP TABLE IF EXISTS public.zonage CASCADE;
        CREATE TABLE public.zonage(
            siret BIGINT PRIMARY KEY NOT NULL,
            unite_controle CHAR(6) NOT NULL
        );

        DROP TABLE IF EXISTS public.conventions_collectives CASCADE;
        CREATE TABLE public.conventions_collectives (
            code_convention_collective CHAR(4) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );

        DROP TABLE IF EXISTS public.categories_juridiques_insee CASCADE;
        CREATE TABLE public.categories_juridiques_insee (
            code_categorie_juridique_insee CHAR(4) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );

        DROP TABLE IF EXISTS public.daily_calendar CASCADE;
        CREATE TABLE public.daily_calendar (
            full_date DATE NOT NULL UNIQUE,
            day_number INT NOT NULL,
            month_number INT NOT NULL,
            year_number INT NOT NULL,
            weekday_number INT NOT NULL,
            public_holiday BOOLEAN DEFAULT NULL
        );

        DROP TABLE IF EXISTS sys.metadata_scripts CASCADE;
        CREATE TABLE sys.metadata_scripts (
            dag_name VARCHAR(100) NOT NULL,
            script_name VARCHAR(100) NOT NULL,
            main_table VARCHAR(100)
        );

        DROP TABLE IF EXISTS sys.current_status CASCADE;
        CREATE TABLE sys.current_status (
            status CHAR(7) NOT NULL DEFAULT 'SUCCESS'
        );

        DROP TABLE IF EXISTS log.processes_logs CASCADE;
        CREATE TABLE log.processes_logs (
            log_id BIGSERIAL PRIMARY KEY NOT NULL,
            process_id BIGINT NOT NULL,
            stage_type CHAR(6) NOT NULL,
            name_schema VARCHAR(100) NOT NULL,
            name_table VARCHAR(100) NOT NULL,
            process_type VARCHAR(100) NOT NULL,
            table_row_count BIGINT NOT NULL,
            stage_time TIMESTAMP NOT NULL
        );

        DROP TABLE IF EXISTS log.scripts_logs CASCADE;
        CREATE TABLE log.scripts_logs (
            log_id BIGSERIAL PRIMARY KEY NOT NULL,
            dag_name VARCHAR(200) NOT NULL,
            script_name VARCHAR(200) NOT NULL,
            stage_type VARCHAR(10) NOT NULL,
            stage_time TIMESTAMP NOT NULL,
            main_table VARCHAR(100),
            table_row_count BIGINT
        );
        DROP TABLE IF EXISTS log.integrations_logs CASCADE;
        CREATE TABLE log.integrations_logs (
            log_id BIGSERIAL PRIMARY KEY NOT NULL,
            declaration_date TIMESTAMP NOT NULL,
            stage_type VARCHAR(10) NOT NULL,
            stage_time TIMESTAMP NOT NULL
        );


        -- les ALTER TABLE suivants s'assurent que tous liens (via les id) entre les tables sont cohérents
        ALTER TABLE public.etablissements ADD FOREIGN KEY (entreprise_id) REFERENCES public.entreprises (entreprise_id) ON DELETE CASCADE;
        ALTER TABLE public.salaries ADD FOREIGN KEY (etablissement_id) REFERENCES public.etablissements (etablissement_id) ON DELETE CASCADE;
        ALTER TABLE public.contrats ADD FOREIGN KEY (salarie_id) REFERENCES public.salaries (salarie_id) ON DELETE CASCADE;
        ALTER TABLE public.contrats ADD FOREIGN KEY (etablissement_id) REFERENCES public.etablissements (etablissement_id) ON DELETE CASCADE;
        ALTER TABLE public.contrats ADD FOREIGN KEY (poste_id) REFERENCES public.postes (poste_id);
        ALTER TABLE public.activites ADD FOREIGN KEY (contrat_id) REFERENCES public.contrats (contrat_id) ON DELETE CASCADE;

        ALTER TABLE public.contrats ADD FOREIGN KEY (code_motif_recours) REFERENCES public.motifs_recours (code_motif_recours);
        ALTER TABLE public.contrats ADD FOREIGN KEY (code_convention_collective) REFERENCES public.conventions_collectives (code_convention_collective);
        ALTER TABLE public.contrats ADD FOREIGN KEY (code_nature_contrat) REFERENCES public.natures_contrats (code_nature_contrat);
        ALTER TABLE public.etablissements ADD FOREIGN KEY (code_categorie_juridique_insee) REFERENCES public.categories_juridiques_insee (code_categorie_juridique_insee);
        ALTER TABLE public.etablissements ADD FOREIGN KEY (code_naf) REFERENCES public.naf (code_naf);
        ALTER TABLE public.etablissements ADD FOREIGN KEY (code_convention_collective) REFERENCES public.conventions_collectives (code_convention_collective);

        -- One plain index to be able to delete a salarie from salaries table easily
        CREATE INDEX to_delete_fast_on_salaries ON public.contrats (salarie_id);

        -- Initialize table chargement_donnees
        INSERT INTO public.chargement_donnees VALUES (NULL);

        -- Initialize table current_status
        INSERT INTO sys.current_status VALUES (DEFAULT);
        