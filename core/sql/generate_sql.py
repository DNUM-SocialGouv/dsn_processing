import argparse
import glob
import os

from dsn_processing._base import Base
from dsn_processing.core.sql.utils import (
    LinkTable,
    MapTable,
    SourceTable,
    TargetTable,
    check_id_consistency_in_link_table,
    combine_queries,
    get_child_load_query,
    get_copy_query,
    get_grandparent_load_query,
    get_parent_load_query,
    get_strong_mapping_query,
    get_update_activites_from_contrats_merge_query,
    get_weak_mapping_query,
    remove_old_data,
    shift_anonymization,
    update_static_table_query,
)

parser = argparse.ArgumentParser(description="Script to generate a SQL task script.")
parser.add_argument(
    "-f",
    "--file",
    type=str,
    help="SQL file name to generate (without extension), generate all files if None.",
)
parser.add_argument(
    "-d",
    "--dag",
    type=str,
    help="Dag ID to store the SQL script, if not specified set the default value for each task.",
)
parser.add_argument("-v", "--verbose", action="store_true", help="print logs")

if __name__ == "__main__":
    args = parser.parse_args()
    base = Base(verbose=args.verbose, log_level="info")

    path = (
        os.environ["DSN_PROCESSING_REPOSITORY_PATH"]
        if "DSN_PROCESSING_REPOSITORY_PATH" in os.environ
        else os.path.join(os.environ["HOME"], "code", "dsn_processing")
    )

    # generate all option (and erase all sql files in sql folder)
    assert (
        args.dag is None if args.file is None else True
    ), "dag must empty if file field is not filled"
    generate_all = True if args.file is None else False
    if generate_all:
        list(
            map(
                os.remove,
                glob.glob(
                    os.path.join(
                        path,
                        "core",
                        "sql",
                        "*",
                        "*.sql",
                    )
                ),
            )
        )

    # function to write sql file
    def write_sql_file(default_dag, default_file, query, to_log=False):
        # file and dag names
        file_name = args.file or default_file
        dag_name = args.dag or default_dag

        # create dag folder
        if not os.path.exists(
            os.path.join(
                path,
                "core",
                "sql",
                dag_name,
            )
        ):
            os.mkdir(
                os.path.join(
                    path,
                    "core",
                    "sql",
                    dag_name,
                )
            )

        before_query = ""
        after_query = ""

        if to_log:
            before_query = f"""
        CALL log.log_script('{dag_name}', '{file_name}', 'BEGIN');
            """
            after_query = f"""
        CALL log.log_script('{dag_name}', '{file_name}', 'END');
            """

        # write file
        with open(
            os.path.join(
                path,
                "core",
                "sql",
                dag_name,
                f"{file_name}.sql",
            ),
            "w",
        ) as file:
            file.write(combine_queries([before_query, query, after_query]))

        # log
        base._log(
            f"generate {file_name}.sql store to {dag_name} DAG ID directory",
            level="info",
        )

    # queries
    if generate_all or args.file == "create_permanent_tables":
        query = """
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
        """

        write_sql_file("init_database", "create_permanent_tables", query)

    if generate_all or args.file == "create_integration_tables":
        query = """

        DROP TABLE IF EXISTS raw.raw_etablissements;
        CREATE TABLE raw.raw_etablissements (
            idetablissement BIGINT PRIMARY KEY NOT NULL,
            iddeclaration BIGINT NOT NULL,
            siren CHAR(9) NOT NULL,
            nicentre CHAR(5),
            codeapen CHAR(5) NOT NULL,
            voieentre VARCHAR(50),
            cpentre CHAR(5),
            localiteentre VARCHAR(50),
            compltdistributionentre VARCHAR(50),
            compltvoieentre VARCHAR(50),
            effectifmoyenentreprisefinperiode INT,
            codepaysentr CHAR(2),
            implantationentreprise CHAR(2),
            datedebperref DATE,
            datefinperref DATE,
            raisonsocialeentr VARCHAR(80),
            nic CHAR(5) NOT NULL,
            codeapet CHAR(5) NOT NULL,
            voieetab VARCHAR(50),
            cp CHAR(5),
            localite VARCHAR(50),
            compltdistributionetab VARCHAR(50),
            compltvoieetab VARCHAR(50),
            effectiffinperiode INT,
            codepays CHAR(2),
            naturejuridiqueemployeur CHAR(2),
            dateclotureexercicecomptable DATE,
            dateadhesiontesecea DATE,
            datesortietesecea DATE,
            codeinseecommune CHAR(5),
            dateecheanceapplique CHAR(8),
            categoriejuridique CHAR(4),
            enseigneetablissement VARCHAR(80),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            codeconvcollectiveapplic CHAR(4),
            codeconvcollectiveprinci CHAR(4), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            oprateurcompetences CHAR(2),
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_salaries;
        CREATE TABLE raw.raw_salaries (
            idindividu BIGINT PRIMARY KEY NOT NULL,
            idetablissement BIGINT NOT NULL,
            nirdeclare CHAR(13),
            nomfamille VARCHAR(80) NOT NULL,
            nomusage VARCHAR(80),
            prenoms VARCHAR(80) NOT NULL,
            codesexe CHAR(2),
            datenaissance CHAR(8) NOT NULL,
            lieunaissance VARCHAR(30), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            voie VARCHAR(50),
            cp CHAR(5),
            localite VARCHAR(50),
            codepays CHAR(2),
            distribution VARCHAR(50),
            codeue CHAR(2), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            codedepartnaissance CHAR(2), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            codepaysnaissance CHAR(2), -- devrait être NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les données
            compltlocalisation VARCHAR(50),
            compltvoie VARCHAR(50),
            mel VARCHAR(100),
            matricule VARCHAR(30),
            ntt VARCHAR(40),
            nombreenfantcharge SMALLINT,
            statutetrangerfiscal CHAR(2),
            embauche CHAR(2),
            niveauformation CHAR(2),
            nir CHAR(13),
            datesngi CHAR(8),
            nomfamillesngi VARCHAR(80),
            prenomssngi VARCHAR(80),
            nommaritalsngi VARCHAR(80),
            coderesultsngi CHAR(2),
            indiccertifsngi NUMERIC,
            comnaisssngi VARCHAR(40),
            codedeptnaisssngi CHAR(3),
            paysnaisssngi CHAR(2),
            datenirsngi CHAR(8),
            datedecessngi CHAR(8),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            estcodifie INT,
            niveaudiplome CHAR(2),
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_contrats;
        CREATE TABLE raw.raw_contrats (
            idcontrat BIGINT PRIMARY KEY NOT NULL,
            idindividu BIGINT NOT NULL,
            datedebut DATE NOT NULL,
            codestatutconv CHAR(2) NOT NULL,
            codestatutcatretraitecompl CHAR(2) NOT NULL,
            codepcsese CHAR(4) NOT NULL,
            codecompltpcsese CHAR(6),
            libelleemploi VARCHAR(120) NOT NULL,
            codenature CHAR(2) NOT NULL,
            codedisppolitiquepublique CHAR(2) NOT NULL,
            numero VARCHAR(20) NOT NULL,
            datefinprev DATE,
            codeunitequotite CHAR(2) NOT NULL,
            quotitetravailcategorie REAL NOT NULL,
            quotitetravailcontrat REAL NOT NULL,
            modaliteexercicetempstravail CHAR(2) NOT NULL,
            complementbaseregimeobligatoire CHAR(2) NOT NULL,
            codeccn CHAR(4) NOT NULL,
            coderegimemaladie CHAR(3) NOT NULL,
            lieutravail VARCHAR(14) NOT NULL,
            coderegimerisquevieillesse CHAR(3) NOT NULL,
            codemotifrecours CHAR(2),
            codecaissecp VARCHAR(20),
            travailleuraetrangerss CHAR(2) NOT NULL,
            codemotifexclusion CHAR(2),
            codestatutemploi CHAR(2) NOT NULL,
            codedelegatairerisquemaladie CHAR(3),
            codeemploimultiple CHAR(2) NOT NULL,
            codeemployeurmultiple CHAR(2) NOT NULL,
            codemetier CHAR(5),
            coderegimerisqueat CHAR(3) NOT NULL,
            coderisqueaccidenttravail CHAR(6) NOT NULL,
            positionconventioncollective VARCHAR(100),
            codestatutcatapecita CHAR(2),
            salarietpspartielcotistpsplein CHAR(2),
            remunerationpourboire CHAR(2),
            siretetabutilisateur VARCHAR(14),
            fpcodecomplpcsese CHAR(4),
            fpnatureposte CHAR(2),
            fpquotitetravailtempscomplet REAL,
            tauxtravailtempspartiel REAL,
            codecatservice CHAR(2),
            fpindicebrut CHAR(4),
            fpindicemajore CHAR(4),
            fpnbi REAL,
            fpindicebrutorigine CHAR(4),
            fpindicebrutcotiemplois CHAR(4),
            fpancemploypublic CHAR(2),
            fpmaintientraitcontractuelt CHAR(4),
            fptypedetachement CHAR(2),
            tauxserviceactif REAL,
            niveauremuneration CHAR(3),
            echelon CHAR(2),
            coefficienthierarchique CHAR(8),
            statutboeth CHAR(2),
            compltdispositifpublic CHAR(2),
            misedispoexterneindividu CHAR(2),
            catclassementfinale CHAR(2),
            collegecnieg CHAR(2),
            amenagtpstravactivparti CHAR(2),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            tauxdeductforffraispro REAL,
            numerointerneepublic VARCHAR(20),
            typegestionac CHAR(2),
            dateadhesion DATE,
            codeaffasschomage CHAR(6),
            statutorgspectacle CHAR(2),
            genrenavigation CHAR(2),
            grade CHAR(5),
            fpindicecti SMALLINT,
            finessgeographique CHAR(9),
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_changements_salaries;
        CREATE TABLE raw.raw_changements_salaries (
            idindividuchangement BIGINT PRIMARY KEY NOT NULL,
            idindividu BIGINT NOT NULL,
            datemodification DATE NOT NULL,
            anciennir CHAR(13),
            nomfamille VARCHAR(80),
            prenoms VARCHAR(80),
            datenaissance CHAR(8),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            estcodifie INT,
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_changements_contrats;
        CREATE TABLE raw.raw_changements_contrats (
            idcontratchangement BIGINT PRIMARY KEY NOT NULL,
            idcontrat BIGINT NOT NULL,
            datemodification DATE NOT NULL,
            codestatutconv CHAR(2),
            codestatutcatretraitecompl CHAR(2),
            codenature CHAR(2),
            codedisppolitiquepublique CHAR(2),
            codeunitequotite CHAR(2),
            quotitetravailcontrat REAL,
            modaliteexercietempstravail CHAR(2),
            complementbaseregimeobligatoire CHAR(2),
            codeccn CHAR(4),
            siretetab CHAR(14),
            lieutravail VARCHAR(14),
            numero VARCHAR(20),
            codemotifrecours CHAR(2),
            travailleuraetrangerss CHAR(2),
            codepcsese CHAR(4),
            codecompltpcsese CHAR(6),
            datedebut DATE,
            quotitetravailcategorie REAL,
            codecaissecp VARCHAR(20),
            coderisque CHAR(6),
            codestatutcatapecita CHAR(2),
            salarietpspartielcotistpsplein CHAR(2),
            profondeurrecalculpaie DATE,
            fpcodecomplpcsese CHAR(4),
            fpnatureposte CHAR(2),
            fpquotitetravailtempscomplet REAL,
            tauxtravailtempspartiel REAL,
            codecatservice CHAR(2),
            fpindicebrut CHAR(4),
            fpindicemajore CHAR(4),
            fpnbi REAL,
            fpindicebrutorigine CHAR(4),
            fpindicebrutcotiemplois CHAR(4),
            fpancemploypublic CHAR(2),
            fpmaintientraitcontractuelt CHAR(4),
            tauxserviceactif REAL,
            niveauremuneration CHAR(3),
            echelon CHAR(2),
            coefficienthierarchique CHAR(8),
            statutboeth CHAR(2),
            compltdispositifpublic CHAR(2),
            misedispoexterneindividu CHAR(2),
            catclassementfinale CHAR(2),
            collegecnieg CHAR(2),
            amenagtpstravactivparti CHAR(2),
            fptypedetachement CHAR(2),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            tauxdeductforffraispro REAL,
            coderegimemaladie CHAR(3),
            coderegimerisquevieillesse CHAR(3),
            positionconventioncollective VARCHAR(100),
            coderisqueaccidenttravail CHAR(3),
            codestatutemploi CHAR(2),
            codeemploimultiple CHAR(2),
            codeemployeurmultiple CHAR(2),
            grade CHAR(5),
            fpindicecti SMALLINT,
            finessgeographique CHAR(9),
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_fins_contrats;
        CREATE TABLE raw.raw_fins_contrats (
            idcontratfin BIGINT PRIMARY KEY NOT NULL,
            idcontrat BIGINT NOT NULL,
            datefin DATE NOT NULL,
            codemotifrupture CHAR(3) NOT NULL,
            dernierjourtrav DATE,
            declarationfincontratusage CHAR(2),
            soldecongesacqnonpris REAL,
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_activites;
        CREATE TABLE raw.raw_activites (
            idactivite BIGINT NOT NULL,
            idremuneration BIGINT NOT NULL,
            typeactivite CHAR(2) NOT NULL,
            mesure REAL NOT NULL,
            unitemesure CHAR(2),
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_remunerations;
        CREATE TABLE raw.raw_remunerations (
            idremuneration BIGINT NOT NULL,
            idversement BIGINT NOT NULL,
            datedebutperiodepaie DATE NOT NULL,
            datefinperiodepaie DATE NOT NULL,
            numerocontrat VARCHAR(20) NOT NULL,
            typeremuneration CHAR(3) NOT NULL,
            nombreheure REAL,
            montant REAL NOT NULL,
            tauxremunpositstatutaire REAL,
            tauxmajorresidentielle REAL,
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            tauxremucotisee REAL,
            tauxmajorationexaexe REAL,
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_versements;
        CREATE TABLE raw.raw_versements (
            idversement BIGINT NOT NULL,
            idindividu BIGINT NOT NULL,
            dateversement DATE NOT NULL,
            remunarationnettefiscale REAL NOT NULL,
            numeroversement CHAR(2),
            montantnetverse REAL NOT NULL,
            tauxprelevsource REAL,
            typetauxprelevsource CHAR(2),
            identauxprelevsource BIGINT,
            montantprelevsource REAL,
            montantpartnonimprevenu REAL,
            montantabattbasefiscale REAL,
            montantdiffasspasetrnf REAL,
            datechargement DATE NOT NULL,
            iddatechargement INT NOT NULL,
            iddeclaration BIGINT NOT NULL,
            datedeclaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS raw.raw_zonage CASCADE;
        CREATE TABLE raw.raw_zonage(
            siret VARCHAR(15),
            unite_controle VARCHAR(6)
        );

        DROP TABLE IF EXISTS source.source_changements_salaries;
        CREATE TABLE source.source_changements_salaries (
            source_salarie_id BIGINT NOT NULL,
            date_modification DATE NOT NULL,
            ancien_nir BIGINT,
            ancien_nom_famille VARCHAR(80),
            anciens_prenoms VARCHAR(80),
            ancienne_date_naissance CHAR(10),
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_changements_contrats;
        CREATE TABLE source.source_changements_contrats (
            source_contrat_id BIGINT NOT NULL,
            date_modification DATE NOT NULL,
            siret_ancien_employeur BIGINT,
            ancien_numero VARCHAR(20),
            ancienne_date_debut DATE,
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_fins_contrats;
        CREATE TABLE source.source_fins_contrats (
            source_contrat_fin_id BIGINT PRIMARY KEY NOT NULL,
            source_contrat_id BIGINT NOT NULL,
            date_fin DATE NOT NULL,
            code_motif_rupture CHAR(3) NOT NULL,
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_entreprises;
        CREATE TABLE source.source_entreprises (
            source_etablissement_id BIGINT PRIMARY KEY NOT NULL,
            entreprise_key BIGINT NOT NULL,
            raison_sociale VARCHAR(80),
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_etablissements;
        CREATE TABLE source.source_etablissements (
            source_etablissement_id BIGINT PRIMARY KEY NOT NULL,
            entreprise_key BIGINT NOT NULL,
            etablissement_key BIGINT NOT NULL,
            siege_social BOOLEAN NOT NULL,
            enseigne VARCHAR(80),
            adresse VARCHAR(50),
            complement_adresse VARCHAR(50),
            code_postal CHAR(5),
            commune VARCHAR(50),
            code_categorie_juridique_insee CHAR(4),
            code_naf CHAR(5) NOT NULL,
            code_convention_collective CHAR(4), -- devrait être  NOT NULL d'après le cahier technique mais ce n'est pas le cas dans les donnéess
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_salaries;
        CREATE TABLE source.source_salaries (
            source_salarie_id BIGINT PRIMARY KEY NOT NULL,
            source_etablissement_id BIGINT NOT NULL,
            salarie_key VARCHAR(170) NOT NULL,
            nir BIGINT,
            nom_famille VARCHAR(80) NOT NULL,
            nom_usage VARCHAR(80),
            prenoms VARCHAR(80) NOT NULL,
            date_naissance CHAR(10) NOT NULL,
            lieu_naissance VARCHAR(30),
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_contrats;
        CREATE TABLE source.source_contrats (
            source_contrat_id BIGINT PRIMARY KEY NOT NULL,
            source_etablissement_id BIGINT NOT NULL,
            source_salarie_id BIGINT NOT NULL,
            contrat_key VARCHAR(46) NOT NULL,
            numero VARCHAR(20) NOT NULL,
            date_debut DATE NOT NULL,
            date_debut_effective DATE,
            poste_id BIGINT NOT NULL,
            code_nature_contrat CHAR(2) NOT NULL,
            code_convention_collective CHAR(4) NOT NULL,
            code_motif_recours CHAR(2),
            code_dispositif_public CHAR(2),
            code_complement_dispositif_public CHAR(2),
            lieu_travail VARCHAR(14),
            code_mise_disposition_externe CHAR(2),
            date_fin_previsionnelle DATE,
            etu_etablissement_key BIGINT,
            etu_id BIGINT,
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.source_activites;
        CREATE TABLE source.source_activites (
            source_remuneration_id BIGINT NOT NULL,
            source_activite_id BIGINT,
            source_contrat_id BIGINT NOT NULL,
            date_debut_paie DATE NOT NULL,
            date_fin_paie DATE NOT NULL,
            type_heures INT NOT NULL,
            nombre_heures REAL NOT NULL,
            date_derniere_declaration DATE NOT NULL
        );

        DROP TABLE IF EXISTS source.link_entreprises;
        CREATE TABLE source.link_entreprises (
            source_etablissement_id BIGINT NOT NULL,
            entreprise_id BIGINT NOT NULL
        );

        DROP TABLE IF EXISTS source.link_etablissements;
        CREATE TABLE source.link_etablissements (
            source_etablissement_id BIGINT NOT NULL,
            etablissement_id BIGINT NOT NULL
        );

        DROP TABLE IF EXISTS source.link_salaries;
        CREATE TABLE source.link_salaries (
            source_salarie_id BIGINT NOT NULL,
            salarie_id BIGINT NOT NULL
        );

        DROP TABLE IF EXISTS source.link_contrats;
        CREATE TABLE source.link_contrats (
            source_contrat_id BIGINT NOT NULL,
            contrat_id BIGINT NOT NULL
        );
        """

        map_changes_salaries = MapTable(
            "source.map_changes_salaries",
            "old_salarie_id",
            "new_salarie_id",
            "date_modification",
        )
        map_changes_salaries_impact_contrats = MapTable(
            "source.map_changes_salaries_impact_contrats",
            "old_contrat_id",
            "new_contrat_id",
            "date_modification",
        )
        map_changes_contrats = MapTable(
            "source.map_changes_contrats",
            "old_contrat_id",
            "new_contrat_id",
            "date_modification",
        )

        query += "\n" + map_changes_salaries.get_create_table_query()
        query += "\n" + map_changes_salaries_impact_contrats.get_create_table_query()
        query += "\n" + map_changes_contrats.get_create_table_query()

        write_sql_file("init_database", "create_integration_tables", query)

    if generate_all or args.file == "create_trigger_logs":
        query = """

        -- create a stored procedure for logs
        CREATE OR REPLACE PROCEDURE log.log_script(var_dag_name VARCHAR, var_script_name VARCHAR, var_stage_type VARCHAR)
        LANGUAGE 'plpgsql'
        AS $log_script$
        DECLARE
            var_stage_time TIMESTAMP;
            var_main_table VARCHAR(100);
            var_table_row_count BIGINT;
        BEGIN
            EXECUTE 'SELECT CLOCK_TIMESTAMP()' INTO var_stage_time;
            INSERT INTO log.scripts_logs(
                dag_name,
                script_name,
                stage_type,
                stage_time,
                main_table,
                table_row_count
            )
            VALUES (
                var_dag_name,
                var_script_name,
                var_stage_type,
                var_stage_time,
                var_main_table,
                var_table_row_count
            );
        END;
        $log_script$;

        -- Create a trigger function to log the start or the end of a process
        CREATE OR REPLACE FUNCTION log.log_process()
        RETURNS TRIGGER
        LANGUAGE 'plpgsql'
        AS $log_process$
        DECLARE
            table_row_count BIGINT;
            stage_time TIMESTAMP;
        BEGIN
            EXECUTE 'SELECT COUNT(1) FROM   ' || TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME  INTO table_row_count;
            EXECUTE 'SELECT CLOCK_TIMESTAMP() ' INTO stage_time;
            INSERT INTO log.processes_logs(
                process_id,
                stage_type,
                name_schema,
                name_table,
                process_type,
                table_row_count,
                stage_time
            ) VALUES (
                TG_RELID,
                TG_WHEN,
                TG_TABLE_SCHEMA,
                TG_TABLE_NAME,
                TG_OP,
                table_row_count,
                stage_time
            );
            RETURN NULL;
        END;
        $log_process$;

        -- Create logging triggers for all existing tables, except for log.processes_logs
        DO
        $$
        DECLARE
            table_infos record;
        BEGIN
            FOR table_infos IN (
                SELECT schemaname, tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname in  ( 'public' , 'raw' , 'source' )
                    AND tablename <> 'processes_logs'
            )

            LOOP
            -- Logging trigger to notify the start of a process
            EXECUTE FORMAT('DROP TRIGGER IF EXISTS log_%s_%s_start ON %s.%s;',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            EXECUTE FORMAT('CREATE TRIGGER log_%s_%s_start BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s.%s FOR EACH STATEMENT EXECUTE FUNCTION log.log_process()',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);

            -- Logging trigger to notify the end of a process
            EXECUTE FORMAT('DROP TRIGGER IF EXISTS log_%s_%s_end ON %s.%s;',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            EXECUTE FORMAT('CREATE TRIGGER log_%s_%s_end AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s.%s FOR EACH STATEMENT EXECUTE FUNCTION log.log_process()',
                            table_infos.schemaname, table_infos.tablename, table_infos.schemaname, table_infos.tablename);
            END LOOP;
        END;
        $$;
        """

        write_sql_file("init_database", "create_trigger_logs", query)

    if generate_all or args.file == "extract_and_load_metadata_scripts":
        query = "\n" + get_copy_query(
            "sys.metadata_scripts", freeze=False, encoding="UTF8"
        )

        write_sql_file(
            "update_database", "extract_and_load_metadata_scripts", query, True
        )

    if generate_all or args.file == "update_calendar":
        query = """
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
        """

        write_sql_file("update_database", "update_calendar", query, True)

    if generate_all or args.file == "extract_etablissements":
        query = get_copy_query("raw.raw_etablissements", source=False)

        write_sql_file("monthly_integration", "extract_etablissements", query, True)

    if generate_all or args.file == "extract_salaries":
        query = get_copy_query("raw.raw_salaries", source=False)

        write_sql_file("monthly_integration", "extract_salaries", query, True)

    if generate_all or args.file == "extract_contrats":
        query = get_copy_query("raw.raw_contrats", source=False)

        write_sql_file("monthly_integration", "extract_contrats", query, True)

    if generate_all or args.file == "extract_changements_salaries":
        query = get_copy_query("raw.raw_changements_salaries", source=False)

        write_sql_file(
            "monthly_integration", "extract_changements_salaries", query, True
        )

    if generate_all or args.file == "transform_changements_salaries":
        query = """
        TRUNCATE TABLE source.source_changements_salaries CASCADE;
        WITH round1 AS (
            SELECT
                idindividuchangement,
                idindividu,
                datemodification,
                anciennir,
                nomfamille,
                prenoms,
                datenaissance,
                datedeclaration
            FROM raw.raw_changements_salaries
        ),

        round2 AS (
            SELECT
                current_.idindividuchangement,
                current_.idindividu,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.anciennir, next_.anciennir) AS anciennir,
                COALESCE(current_.nomfamille, next_.nomfamille) AS nomfamille,
                COALESCE(current_.prenoms, next_.prenoms) AS prenoms,
                COALESCE(current_.datenaissance, next_.datenaissance) AS datenaissance,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round1 AS current_
            LEFT JOIN raw.raw_changements_salaries AS next_
                ON current_.idindividu = next_.idindividu
                AND COALESCE(current_.anciennir, 'NULL') = COALESCE(next_.anciennir, 'NULL')
                AND (
                    ((current_.nomfamille IS NULL OR next_.nomfamille IS NULL)
                        AND (current_.prenoms IS NULL OR next_.prenoms IS NULL)
                        AND (current_.datenaissance IS NULL OR next_.datenaissance IS NULL))
                    OR current_.idindividuchangement = next_.idindividuchangement
                )
        ),

        round3 AS (
            SELECT
                current_.idindividu,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.anciennir, next_.anciennir) AS anciennir,
                COALESCE(current_.nomfamille, next_.nomfamille) AS nomfamille,
                COALESCE(current_.prenoms, next_.prenoms) AS prenoms,
                COALESCE(current_.datenaissance, next_.datenaissance) AS datenaissance,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round2 AS current_
            LEFT JOIN raw.raw_changements_salaries AS next_
                ON current_.idindividu = next_.idindividu
                AND COALESCE(current_.anciennir, 'NULL') = COALESCE(next_.anciennir, 'NULL')
                AND (
                    ((current_.nomfamille IS NULL OR next_.nomfamille IS NULL)
                        AND (current_.prenoms IS NULL OR next_.prenoms IS NULL)
                        AND (current_.datenaissance IS NULL OR next_.datenaissance IS NULL))
                    OR current_.idindividuchangement = next_.idindividuchangement
                )
        )

        INSERT INTO source.source_changements_salaries (
            source_salarie_id,
            date_modification,
            ancien_nir,
            ancien_nom_famille,
            anciens_prenoms,
            ancienne_date_naissance,
            date_derniere_declaration
        )
        SELECT DISTINCT
            idindividu AS source_salarie_id,
            datemodification AS date_modification,
            CAST(anciennir AS BIGINT) AS ancien_nir,
            UPPER(TRIM(nomfamille)) AS ancien_nom_famille,
            UPPER(TRIM(prenoms)) AS anciens_prenoms,
            OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0) AS ancienne_date_naissance,
            datedeclaration AS date_derniere_declaration
        FROM round3
        WHERE anciennir IS NOT NULL
            OR nomfamille IS NOT NULL
            OR prenoms IS NOT NULL
            OR datenaissance IS NOT NULL;
        """

        write_sql_file(
            "monthly_integration", "transform_changements_salaries", query, True
        )

    if generate_all or args.file == "extract_changements_contrats":
        query = get_copy_query("raw.raw_changements_contrats", source=False)

        write_sql_file(
            "monthly_integration", "extract_changements_contrats", query, True
        )

    if generate_all or args.file == "transform_changements_contrats":
        query = """
        TRUNCATE TABLE source.source_changements_contrats CASCADE;
        WITH round1 AS (
            SELECT
                idcontratchangement,
                idcontrat,
                datemodification,
                siretetab,
                numero,
                datedebut,
                datedeclaration
            FROM raw.raw_changements_contrats
        ),

        round2 AS (
            SELECT
                current_.idcontratchangement,
                current_.idcontrat,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.siretetab, next_.siretetab) AS siretetab,
                COALESCE(current_.numero, next_.numero) AS numero,
                COALESCE(current_.datedebut, next_.datedebut) AS datedebut,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round1 AS current_
            LEFT JOIN raw.raw_changements_contrats AS next_
                ON current_.idcontrat = next_.idcontrat
                AND (
                    ((current_.siretetab IS NULL OR next_.siretetab IS NULL)
                        AND (current_.numero IS NULL OR next_.numero IS NULL)
                        AND (current_.datedebut IS NULL OR next_.datedebut IS NULL))
                    OR current_.idcontratchangement = next_.idcontratchangement
                )
        ),

        round3 AS (
            SELECT
                current_.idcontrat,
                GREATEST(current_.datemodification, next_.datemodification) AS datemodification,
                COALESCE(current_.siretetab, next_.siretetab) AS siretetab,
                COALESCE(current_.numero, next_.numero) AS numero,
                COALESCE(current_.datedebut, next_.datedebut) AS datedebut,
                GREATEST(current_.datedeclaration, next_.datedeclaration) AS datedeclaration
            FROM round2 AS current_
            LEFT JOIN raw.raw_changements_contrats AS next_
                ON current_.idcontrat = next_.idcontrat
                AND (
                    ((current_.siretetab IS NULL OR next_.siretetab IS NULL)
                        AND (current_.numero IS NULL OR next_.numero IS NULL)
                        AND (current_.datedebut IS NULL OR next_.datedebut IS NULL))
                    OR current_.idcontratchangement = next_.idcontratchangement
                )
        )

        INSERT INTO source.source_changements_contrats (
            source_contrat_id,
            date_modification,
            siret_ancien_employeur,
            ancien_numero,
            ancienne_date_debut,
            date_derniere_declaration
        )
        SELECT DISTINCT
            idcontrat AS source_contrat_id,
            datemodification AS date_modification,
            CAST(siretetab AS BIGINT) AS siret_ancien_employeur,
            UPPER(TRIM(numero)) AS ancien_numero,
            datedebut AS ancienne_date_debut,
            datedeclaration AS date_derniere_declaration
        FROM round3
        WHERE siretetab IS NOT NULL
            OR numero IS NOT NULL
            OR datedebut IS NOT NULL;
        """

        write_sql_file(
            "monthly_integration", "transform_changements_contrats", query, True
        )

    if generate_all or args.file == "extract_fins_contrats":
        query = get_copy_query("raw.raw_fins_contrats", source=False)

        write_sql_file("monthly_integration", "extract_fins_contrats", query, True)

    if generate_all or args.file == "transform_fins_contrats":
        query = """
        TRUNCATE TABLE source.source_fins_contrats CASCADE;
        INSERT INTO source.source_fins_contrats (
            source_contrat_fin_id,
            source_contrat_id,
            date_fin,
            code_motif_rupture,
            date_derniere_declaration
        ) SELECT
            idcontratfin AS source_contrat_fin_id,
            idcontrat AS source_contrat_id,
            datefin AS date_fin,
            codemotifrupture AS code_motif_rupture,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_fins_contrats;
        """

        write_sql_file("monthly_integration", "transform_fins_contrats", query, True)

    if generate_all or args.file == "transform_entreprises":
        query = """
        TRUNCATE TABLE source.source_entreprises CASCADE;
        INSERT INTO source.source_entreprises (
            source_etablissement_id,
            entreprise_key,
            raison_sociale,
            date_derniere_declaration
        ) SELECT
            idetablissement AS source_etablissement_id,
            CAST(siren AS BIGINT) AS entreprise_key,
            raisonsocialeentr AS raison_sociale,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_etablissements;
        """

        write_sql_file("monthly_integration", "transform_entreprises", query, True)

    if generate_all or args.file == "transform_etablissements":
        query = """
        TRUNCATE TABLE source.source_etablissements CASCADE;
        INSERT INTO source.source_etablissements (
            source_etablissement_id,
            entreprise_key,
            etablissement_key,
            siege_social,
            enseigne,
            adresse,
            complement_adresse,
            code_postal,
            commune,
            code_categorie_juridique_insee,
            code_naf,
            code_convention_collective,
            date_derniere_declaration
        ) SELECT
            idetablissement AS source_etablissement_id,
            CAST(siren AS BIGINT) AS entreprise_key,
            CAST(CONCAT(siren, nic) AS BIGINT) AS etablissement_key,
            COALESCE(nicentre = nic, FALSE) AS siege_social,
            enseigneetablissement AS enseigne,
            voieetab AS adresse,
            compltvoieetab AS complement_adresse,
            cp AS commune_postal,
            localite AS commune,
            categoriejuridique AS code_categorie_juridique_insee,
            codeapet AS code_naf,
            codeconvcollectiveprinci AS code_convention_collective,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_etablissements;
        """

        write_sql_file("monthly_integration", "transform_etablissements", query, True)

    if generate_all or args.file == "transform_salaries":
        query = """
        TRUNCATE TABLE source.source_salaries CASCADE;
        INSERT INTO source.source_salaries (
            source_salarie_id,
            source_etablissement_id,
            salarie_key,
            nir,
            nom_famille,
            nom_usage,
            prenoms,
            date_naissance,
            lieu_naissance,
            date_derniere_declaration
        ) SELECT
            idindividu AS source_salarie_id,
            idetablissement AS source_etablissement_id,
            COALESCE(nirdeclare, CONCAT(UPPER(TRIM(nomfamille)), UPPER(TRIM(prenoms)), OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0))) AS salarie_key,
            CAST(nirdeclare AS BIGINT) AS nir,
            UPPER(TRIM(nomfamille)) AS nom_famille,
            UPPER(TRIM(nomusage)) AS nom_usage,
            UPPER(TRIM(prenoms)) AS prenoms,
            OVERLAY(OVERLAY(datenaissance PLACING '-' FROM 3 FOR 0) PLACING '-' FROM 6 FOR 0) AS date_naissance,
            lieunaissance AS lieu_naissance,
            datedeclaration AS date_derniere_declaration
        FROM raw.raw_salaries;
        """

        write_sql_file("monthly_integration", "transform_salaries", query, True)

    if generate_all or args.file == "transform_contrats":
        query = """
        TRUNCATE TABLE source.source_contrats CASCADE;
        INSERT INTO source.source_contrats (
            source_contrat_id,
            source_etablissement_id,
            source_salarie_id,
            contrat_key,
            numero,
            date_debut,
            poste_id,
            code_nature_contrat,
            code_convention_collective,
            code_motif_recours,
            code_dispositif_public,
            code_complement_dispositif_public,
            lieu_travail,
            code_mise_disposition_externe,
            date_fin_previsionnelle,
            etu_etablissement_key,
            etu_id,
            date_derniere_declaration
        ) SELECT
            raw.raw_contrats.idcontrat AS source_contrat_id,
            source.source_salaries.source_etablissement_id AS source_etablissement_id,
            raw.raw_contrats.idindividu AS source_salarie_id,
            CONCAT(UPPER(TRIM(raw.raw_contrats.numero)), '_', CAST(raw.raw_contrats.datedebut AS CHAR(10))) AS contrat_key,
            UPPER(TRIM(raw.raw_contrats.numero)) AS numero,
            raw.raw_contrats.datedebut AS date_debut,
            public.postes.poste_id AS poste_id,
            raw.raw_contrats.codenature AS code_nature_contrat,
            raw.raw_contrats.codeccn AS code_convention_collective,
            raw.raw_contrats.codemotifrecours AS code_motif_recours,
            raw.raw_contrats.codedisppolitiquepublique AS code_dispositif_public,
            raw.raw_contrats.compltdispositifpublic AS code_complement_dispositif_public,
            raw.raw_contrats.lieutravail AS lieu_travail,
            raw.raw_contrats.misedispoexterneindividu AS code_mise_disposition_externe,
            raw.raw_contrats.datefinprev AS date_fin_previsionnelle,
            CASE WHEN (LENGTH(raw.raw_contrats.siretetabutilisateur) = 14
                AND REGEXP_LIKE(raw.raw_contrats.siretetabutilisateur, '^[0-9]*$'))
                THEN CAST(raw.raw_contrats.siretetabutilisateur AS BIGINT)
            END AS etu_etablissement_key,
            NULL AS etu_id,
            raw.raw_contrats.datedeclaration AS date_derniere_declaration
        FROM raw.raw_contrats
        INNER JOIN source.source_salaries
            ON raw.raw_contrats.idindividu = source.source_salaries.source_salarie_id
        INNER JOIN public.postes
            ON public.postes.poste_key = UPPER(TRIM(raw.raw_contrats.libelleemploi));

        -- Ajout de l'id de l'ETU dans la table source si le siret renseigné match un siret de la table etablissements
        UPDATE source.source_contrats
        SET
            etu_id = public.etablissements.etablissement_id
        FROM public.etablissements
        WHERE source.source_contrats.etu_etablissement_key = public.etablissements.etablissement_key;
        """

        write_sql_file("monthly_integration", "transform_contrats", query, True)

    if generate_all or args.file == "load_entreprises":
        source = SourceTable(
            name="source.source_entreprises",
            id_column="source_etablissement_id",
            parent_id_columns=[],
            key_column="entreprise_key",
            auxiliary_columns=["raison_sociale", "date_derniere_declaration"],
            order_columns=["date_derniere_declaration"],
        )
        target = TargetTable(
            name="public.entreprises",
            id_column="entreprise_id",
            parent_id_columns=[],
            key_column="entreprise_key",
            auxiliary_columns=["raison_sociale", "date_derniere_declaration"],
        )
        link = LinkTable(
            name="source.link_entreprises",
            source_id_column="source_etablissement_id",
            target_id_column="entreprise_id",
        )
        query = get_grandparent_load_query(
            source=source, target=target, link=link, add_date_premiere_declaration=True
        )

        write_sql_file("monthly_integration", "load_entreprises", query, True)

    if generate_all or args.file == "load_etablissements":
        source = SourceTable(
            name="source.source_etablissements",
            id_column="source_etablissement_id",
            parent_id_columns=["source_etablissement_id"],
            key_column="etablissement_key",
            auxiliary_columns=[
                "siege_social",
                "enseigne",
                "adresse",
                "complement_adresse",
                "code_postal",
                "commune",
                "code_categorie_juridique_insee",
                "code_naf",
                "code_convention_collective",
                "date_derniere_declaration",
            ],
            order_columns=["date_derniere_declaration"],
        )
        target = TargetTable(
            name="public.etablissements",
            id_column="etablissement_id",
            parent_id_columns=["entreprise_id"],
            key_column="etablissement_key",
            auxiliary_columns=[
                "siege_social",
                "enseigne",
                "adresse",
                "complement_adresse",
                "code_postal",
                "commune",
                "code_categorie_juridique_insee",
                "code_naf",
                "code_convention_collective",
                "date_derniere_declaration",
            ],
        )
        link = LinkTable(
            name="source.link_etablissements",
            source_id_column="source_etablissement_id",
            target_id_column="etablissement_id",
        )
        grandparent_link = LinkTable(
            name="source.link_entreprises",
            source_id_column="source_etablissement_id",
            target_id_column="entreprise_id",
        )

        query = get_parent_load_query(
            source=source,
            target=target,
            link=link,
            grandparent_link=grandparent_link,
            add_date_premiere_declaration=True,
        )

        write_sql_file("monthly_integration", "load_etablissements", query, True)

    if generate_all or args.file == "load_salaries":
        source = SourceTable(
            name="source.source_salaries",
            id_column="source_salarie_id",
            parent_id_columns=["source_etablissement_id"],
            key_column="salarie_key",
            auxiliary_columns=[
                "nir",
                "nom_famille",
                "nom_usage",
                "prenoms",
                "date_naissance",
                "lieu_naissance",
                "date_derniere_declaration",
            ],
            order_columns=["date_derniere_declaration"],
        )
        target = TargetTable(
            name="public.salaries",
            id_column="salarie_id",
            parent_id_columns=["etablissement_id"],
            key_column="salarie_key",
            auxiliary_columns=[
                "nir",
                "nom_famille",
                "nom_usage",
                "prenoms",
                "date_naissance",
                "lieu_naissance",
                "date_derniere_declaration",
            ],
        )
        link = LinkTable(
            name="source.link_salaries",
            source_id_column="source_salarie_id",
            target_id_column="salarie_id",
        )
        grandparent_link = LinkTable(
            name="source.link_etablissements",
            source_id_column="source_etablissement_id",
            target_id_column="etablissement_id",
        )

        query = get_parent_load_query(
            source=source,
            target=target,
            link=link,
            grandparent_link=grandparent_link,
        )

        write_sql_file("monthly_integration", "load_salaries", query, True)

    if generate_all or args.file == "load_contrats":
        source = SourceTable(
            name="source.source_contrats",
            id_column="source_contrat_id",
            parent_id_columns=["source_etablissement_id", "source_salarie_id"],
            key_column="contrat_key",
            auxiliary_columns=[
                "date_debut",
                "numero",
                "poste_id",
                "code_nature_contrat",
                "code_convention_collective",
                "code_motif_recours",
                "code_dispositif_public",
                "code_complement_dispositif_public",
                "lieu_travail",
                "code_mise_disposition_externe",
                "date_fin_previsionnelle",
                "etu_id",
                "date_derniere_declaration",
            ],
            order_columns=["date_derniere_declaration"],
        )
        target = TargetTable(
            name="public.contrats",
            id_column="contrat_id",
            parent_id_columns=["etablissement_id", "salarie_id"],
            key_column="contrat_key",
            auxiliary_columns=[
                "date_debut",
                "numero",
                "poste_id",
                "code_nature_contrat",
                "code_convention_collective",
                "code_motif_recours",
                "code_dispositif_public",
                "code_complement_dispositif_public",
                "lieu_travail",
                "code_mise_disposition_externe",
                "date_fin_previsionnelle",
                "etu_id",
                "date_derniere_declaration",
            ],
        )
        link = LinkTable(
            name="source.link_contrats",
            source_id_column="source_contrat_id",
            target_id_column="contrat_id",
        )
        parent_link = LinkTable(
            name="source.link_salaries",
            source_id_column="source_salarie_id",
            target_id_column="salarie_id",
        )
        grandparent_link = LinkTable(
            name="source.link_etablissements",
            source_id_column="source_etablissement_id",
            target_id_column="etablissement_id",
        )

        query = get_child_load_query(
            source=source,
            target=target,
            link=link,
            grandparent_link=grandparent_link,
            parent_link=parent_link,
        )

        write_sql_file("monthly_integration", "load_contrats", query, True)

    if generate_all or args.file == "remove_stt":
        query = """
        -- delete allocated salaries in the etu
        DELETE FROM public.salaries
        WHERE public.salaries.allocated_from_ett IS TRUE;
        """

        write_sql_file("monthly_integration", "remove_stt", query, True)

    if generate_all or args.file == "remove_ctt":
        query = """
        -- delete allocated contrats in the etu
        DELETE FROM public.contrats
        WHERE public.contrats.ett_contrat_id IS NOT NULL;
        """

        write_sql_file("monthly_integration", "remove_ctt", query, True)

    if generate_all or args.file == "allocate_stt":
        query = f"""
        -- stt means "salarié de travail temporaire"
        WITH stt AS (
            SELECT
                c.etu_id AS etablissement_id,
                s.salarie_key,
                s.nir,
                s.nom_famille,
                s.nom_usage,
                s.prenoms,
                s.date_naissance,
                s.lieu_naissance,
                TRUE AS allocated_from_ett,
                s.date_derniere_declaration
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON s.salarie_id = c.salarie_id
            INNER JOIN public.etablissements AS e
                ON e.etablissement_id = c.etablissement_id
            WHERE e.code_naf = '7820Z' AND c.code_nature_contrat = '03' AND c.etu_id IS NOT NULL
        ),

        -- remove duplicates (duplicates are possible since there is no uniqueness constraint on the tuple (etu_id, salarie_key))
        sttwithoutduplicates AS (
            SELECT
                etablissement_id,
                salarie_key,
                nir,
                nom_famille,
                nom_usage,
                prenoms,
                date_naissance,
                lieu_naissance,
                allocated_from_ett,
                date_derniere_declaration
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY etablissement_id, salarie_key ORDER BY 1) AS row_num
                FROM stt) AS ord
            WHERE row_num = 1
        )

        -- insert stt which do not already exist in etu
        INSERT INTO public.salaries (
            etablissement_id,
            salarie_key,
            nir,
            nom_famille,
            nom_usage,
            prenoms,
            date_naissance,
            lieu_naissance,
            allocated_from_ett,
            date_derniere_declaration
        )
        SELECT *
        FROM sttwithoutduplicates
        ON CONFLICT ON CONSTRAINT uk_salarie DO NOTHING;
        """

        write_sql_file("monthly_integration", "allocate_stt", query, True)

    if generate_all or args.file == "allocate_ctt":
        query = f"""
        -- ctt means "contrat de travail temporaire"
        WITH ctt AS (
            SELECT
                c.etu_id AS etablissement_id,
                s.salarie_key,
                CONCAT(c.numero, '_', e.etablissement_key, '_', CAST(c.date_debut AS CHAR(10))) AS contrat_key,
                CONCAT(c.numero, '_', e.etablissement_key) AS numero,
                c.date_debut,
                c.poste_id,
                c.code_nature_contrat,
                c.code_convention_collective,
                c.code_motif_recours,
                c.code_dispositif_public,
                c.code_complement_dispositif_public,
                c.lieu_travail,
                c.code_mise_disposition_externe,
                c.date_fin_previsionnelle,
                c.date_fin_effective,
                c.statut_fin,
                c.contrat_id AS ett_contrat_id,
                c.date_derniere_declaration
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON s.salarie_id = c.salarie_id
            INNER JOIN public.etablissements AS e
                ON e.etablissement_id = c.etablissement_id
            WHERE e.code_naf = '7820Z' AND c.code_nature_contrat = '03' AND c.etu_id IS NOT NULL
        )

        -- insert ctt in contrats (knowing that stt have already been pushed in salaries)
        INSERT INTO public.contrats (
            etablissement_id,
            salarie_id,
            contrat_key,
            numero,
            date_debut,
            poste_id,
            code_nature_contrat,
            code_convention_collective,
            code_motif_recours,
            code_dispositif_public,
            code_complement_dispositif_public,
            lieu_travail,
            code_mise_disposition_externe,
            date_fin_previsionnelle,
            date_fin_effective,
            statut_fin,
            ett_contrat_id,
            date_derniere_declaration)

        (
            SELECT
                ctt.etablissement_id,
                s.salarie_id,
                ctt.contrat_key,
                ctt.numero,
                ctt.date_debut,
                ctt.poste_id,
                ctt.code_nature_contrat,
                ctt.code_convention_collective,
                ctt.code_motif_recours,
                ctt.code_dispositif_public,
                ctt.code_complement_dispositif_public,
                ctt.lieu_travail,
                ctt.code_mise_disposition_externe,
                ctt.date_fin_previsionnelle,
                ctt.date_fin_effective,
                ctt.statut_fin,
                ctt.ett_contrat_id,
                ctt.date_derniere_declaration
            FROM ctt
            INNER JOIN public.salaries AS s
                ON ctt.etablissement_id = s.etablissement_id
                AND ctt.salarie_key = s.salarie_key)
        ON CONFLICT ON CONSTRAINT uk_contrat DO NOTHING;
        """

        write_sql_file("monthly_integration", "allocate_ctt", query, True)

    if generate_all or args.file == "modify_changements_salaries":
        map_changes_salaries = MapTable(
            "source.map_changes_salaries",
            "old_salarie_id",
            "new_salarie_id",
            "date_modification",
        )
        map_changes_salaries_impact_contrats = MapTable(
            "source.map_changes_salaries_impact_contrats",
            "old_contrat_id",
            "new_contrat_id",
            "date_modification",
        )

        queries = [
            f"""
        TRUNCATE TABLE {map_changes_salaries.table_name} CASCADE;

        -- to handle case where employee was declared without NIR and now with a NIR
        INSERT INTO {map_changes_salaries.table_name} (
            {map_changes_salaries.merged_id_column},
            {map_changes_salaries.absorbing_id_column},
            {map_changes_salaries.modification_date_column}
        )

        SELECT
            old_.salarie_id AS old_salarie_id,
            new_.salarie_id AS new_salarie_id,
            new_.date_derniere_declaration AS date_modification
        FROM public.salaries AS new_
        INNER JOIN public.salaries AS old_
            ON new_.salarie_id != old_.salarie_id
            AND new_.etablissement_id = old_.etablissement_id
            AND old_.nir IS NULL
            AND new_.nom_famille = old_.nom_famille
            AND new_.prenoms = old_.prenoms
            AND new_.date_naissance = old_.date_naissance
            AND new_.date_derniere_declaration >= old_.date_derniere_declaration
        ORDER BY new_.date_derniere_declaration DESC;


        -- add declared changes
        INSERT INTO {map_changes_salaries.table_name} (
            {map_changes_salaries.merged_id_column},
            {map_changes_salaries.absorbing_id_column},
            {map_changes_salaries.modification_date_column}
        )

        SELECT
            salaries_old.salarie_id,
            salaries_new.salarie_id,
            changements_salaries.date_modification
        FROM source.source_changements_salaries AS changements_salaries
        INNER JOIN source.link_salaries AS link
            ON changements_salaries.source_salarie_id = link.source_salarie_id
        INNER JOIN public.salaries AS salaries_new
            ON salaries_new.salarie_id = link.salarie_id
        INNER JOIN public.salaries AS salaries_old
            ON salaries_new.etablissement_id = salaries_old.etablissement_id
            AND salaries_new.salarie_id != salaries_old.salarie_id
            AND (
                COALESCE(changements_salaries.ancien_nir, salaries_new.nir) = salaries_old.nir
                AND (
                    COALESCE(changements_salaries.ancien_nom_famille, salaries_new.nom_famille) = salaries_old.nom_famille
                    AND COALESCE(changements_salaries.anciens_prenoms, salaries_new.prenoms) = salaries_old.prenoms
                    AND COALESCE(changements_salaries.ancienne_date_naissance, salaries_new.date_naissance) = salaries_old.date_naissance
                )
            )
        WHERE changements_salaries.ancien_nir IS NOT NULL
            OR changements_salaries.ancien_nom_famille IS NOT NULL
            OR changements_salaries.anciens_prenoms IS NOT NULL
            OR changements_salaries.ancienne_date_naissance IS NOT NULL
        ORDER BY changements_salaries.date_modification DESC;
        """
        ]
        queries.append(map_changes_salaries.get_clean_table_query())

        queries.append(
            f"""
        -- map table on contrats to reflect changes on salaries to contrats
        TRUNCATE TABLE {map_changes_salaries_impact_contrats.table_name} CASCADE;
        INSERT INTO {map_changes_salaries_impact_contrats.table_name} (
            {map_changes_salaries_impact_contrats.merged_id_column},
            {map_changes_salaries_impact_contrats.absorbing_id_column},
            {map_changes_salaries_impact_contrats.modification_date_column}
        )
        SELECT
            contrats_old.contrat_id,
            COALESCE(contrats_new.contrat_id, contrats_old.contrat_id) AS absorbing_id_column,
            map_changes_salaries.{map_changes_salaries.modification_date_column}
        FROM {map_changes_salaries.table_name} AS map_changes_salaries
        INNER JOIN public.contrats AS contrats_old
            ON map_changes_salaries.{map_changes_salaries.merged_id_column} = contrats_old.salarie_id
        LEFT JOIN public.contrats AS contrats_new
            ON map_changes_salaries.{map_changes_salaries.absorbing_id_column} = contrats_new.salarie_id
            AND contrats_new.contrat_key = contrats_old.contrat_key;

        -- patch to handle case salarie1 -> salarie3 and salarie2 -> salarie3 and salarie1 and salarie2 have "same" contrat
        WITH ordered_changes_contrats AS (
            SELECT
                contrats_old.contrat_id,
                map_changes_salaries.new_salarie_id,
                contrats_old.contrat_key AS old_contrat_key,
                map_changes_salaries.{map_changes_salaries.modification_date_column},
                ROW_NUMBER() OVER(PARTITION BY map_changes_salaries.{map_changes_salaries.absorbing_id_column}, contrats_old.contrat_key ORDER BY map_changes_salaries.{map_changes_salaries.modification_date_column} ASC) AS change_nb
            FROM {map_changes_salaries.table_name} AS map_changes_salaries
            INNER JOIN public.contrats AS contrats_old
                ON map_changes_salaries.{map_changes_salaries.merged_id_column} = contrats_old.salarie_id
        )

        INSERT INTO {map_changes_salaries_impact_contrats.table_name} (
            {map_changes_salaries_impact_contrats.merged_id_column},
            {map_changes_salaries_impact_contrats.absorbing_id_column},
            {map_changes_salaries_impact_contrats.modification_date_column}
        )
        SELECT
            merged.contrat_id AS old_contrat_id,
            absorbing.contrat_id AS new_contrat_id,
            merged.date_modification
        FROM ordered_changes_contrats AS merged
        INNER JOIN ordered_changes_contrats AS absorbing
            ON merged.new_salarie_id = absorbing.new_salarie_id
            AND merged.old_contrat_key = absorbing.old_contrat_key
            AND merged.change_nb = absorbing.change_nb + 1;
        """
        )
        queries.append(map_changes_salaries_impact_contrats.get_clean_table_query())
        queries.append(
            get_update_activites_from_contrats_merge_query(
                map_changes_salaries_impact_contrats
            )
        )
        queries.append(
            get_weak_mapping_query(
                "source.link_contrats",
                "contrat_id",
                map_changes_salaries_impact_contrats,
            )
        )
        queries.append(
            get_weak_mapping_query(
                "source.link_salaries", "salarie_id", map_changes_salaries
            )
        )
        queries.append(
            get_strong_mapping_query(
                "public.contrats", "contrat_id", map_changes_salaries_impact_contrats
            )
        )
        queries.append(
            get_weak_mapping_query(
                "public.contrats", "salarie_id", map_changes_salaries
            )
        )
        queries.append(
            get_strong_mapping_query(
                "public.salaries", "salarie_id", map_changes_salaries
            )
        )
        query = combine_queries(queries)

        write_sql_file(
            "monthly_integration", "modify_changements_salaries", query, True
        )

    if generate_all or args.file == "modify_changements_contrats":
        map_changes_contrats = MapTable(
            "source.map_changes_contrats",
            "old_contrat_id",
            "new_contrat_id",
            "date_modification",
        )

        queries = [
            f"""
        TRUNCATE TABLE {map_changes_contrats.table_name} CASCADE;
        INSERT INTO {map_changes_contrats.table_name} (
            {map_changes_contrats.merged_id_column},
            {map_changes_contrats.absorbing_id_column},
            {map_changes_contrats.modification_date_column}
        )
        SELECT
            contrats_old.contrat_id AS id_old_row,
            contrats_new.contrat_id AS id_new_row,
            changements_contrats.date_modification
        FROM source.source_changements_contrats AS changements_contrats
        INNER JOIN source.link_contrats AS link
            ON changements_contrats.source_contrat_id = link.source_contrat_id
        INNER JOIN public.contrats AS contrats_new
            ON contrats_new.contrat_id = link.contrat_id
        INNER JOIN public.contrats AS contrats_old
            ON contrats_new.etablissement_id = contrats_old.etablissement_id
            AND contrats_new.salarie_id = contrats_old.salarie_id
            AND COALESCE(changements_contrats.ancienne_date_debut, contrats_new.date_debut) = contrats_old.date_debut
            AND COALESCE(changements_contrats.ancien_numero, contrats_new.numero) = contrats_old.numero
        WHERE changements_contrats.ancienne_date_debut IS NOT NULL
            OR changements_contrats.ancien_numero IS NOT NULL
        ORDER BY changements_contrats.date_modification;
        """
        ]
        queries.append(map_changes_contrats.get_clean_table_query())
        queries.append(
            get_update_activites_from_contrats_merge_query(map_changes_contrats)
        )
        queries.append(
            get_weak_mapping_query(
                "source.link_contrats", "contrat_id", map_changes_contrats
            )
        )
        queries.append(
            get_strong_mapping_query(
                "public.contrats", "contrat_id", map_changes_contrats
            )
        )
        query = combine_queries(queries)

        write_sql_file(
            "monthly_integration", "modify_changements_contrats", query, True
        )

    if generate_all or args.file == "modify_debuts_contrats":
        query = """
        -- INFER EFFECTIVE START DATES IN CASE OF EMPLOYER CHANGES FOR NEW EMPLOYER'S CONTRATS
        -- get contrats which has been transfered (info in changements_contrats)
        WITH contrats_transferes AS (
            SELECT
                contrat_new.contrat_id,
                changements_contrats.date_modification AS date_debut_effective
            FROM source.source_changements_contrats AS changements_contrats
            INNER JOIN source.link_contrats AS link
                ON changements_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats AS contrat_new
                ON contrat_new.contrat_id = link.contrat_id
            INNER JOIN public.etablissements AS etablissement_new
                ON contrat_new.etablissement_id = etablissement_new.etablissement_id
            WHERE changements_contrats.siret_ancien_employeur IS NOT NULL
                AND etablissement_new.etablissement_key != changements_contrats.siret_ancien_employeur
                AND changements_contrats.date_modification != contrat_new.date_debut
        )

        -- set date_debut_effetive at the DATE of change in new employer
        UPDATE public.contrats
        SET date_debut_effective = contrats_transferes.date_debut_effective
        FROM contrats_transferes
        WHERE contrats_transferes.contrat_id = public.contrats.contrat_id;
        """

        write_sql_file("monthly_integration", "modify_debuts_contrats", query, True)

    if generate_all or args.file == "modify_fins_contrats":
        query = """
        -- DECLARED END DATES
        -- get end date linked with contrat_id in the database
        WITH dates_fin AS (
            SELECT
                public.contrats.contrat_id,
                fins_contrats.date_fin AS date_fin,
                fins_contrats.code_motif_rupture AS motif_fin
            FROM source.source_fins_contrats AS fins_contrats
            INNER JOIN source.link_contrats AS link
                ON fins_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats
                ON link.contrat_id = public.contrats.contrat_id
        )

        -- update date_fin_effective, knowing that motif_fin = '099' means a cancelling of the end DATE
        UPDATE public.contrats
        SET date_fin_effective = CASE WHEN (fins.motif_fin = '099') THEN NULL ELSE fins.date_fin END,
            statut_fin = CASE WHEN fins.motif_fin = '099' THEN NULL ELSE 2 END
        FROM dates_fin AS fins
        WHERE public.contrats.contrat_id = fins.contrat_id;

        -- INFER END DATES BY LACK OF DECLARATIONS
        UPDATE public.contrats
        SET date_fin_effective = (
            CASE WHEN dernieres_declarations.date_fin_previsionnelle IS NOT NULL
                AND dernieres_declarations.date_fin_previsionnelle >= dernieres_declarations.premier_jour_mois_derniere_declaration_contrat
                AND dernieres_declarations.date_fin_previsionnelle <= dernieres_declarations.dernier_jour_mois_derniere_declaration_contrat
                THEN dernieres_declarations.date_fin_previsionnelle
                ELSE dernieres_declarations.dernier_jour_mois_derniere_declaration_contrat END
            ),
            statut_fin = 1
        FROM (
            SELECT
                public.contrats.contrat_id,
                public.contrats.date_derniere_declaration AS premier_jour_mois_derniere_declaration_contrat,
                CAST(DATE_TRUNC('MONTH', public.contrats.date_derniere_declaration) + INTERVAL '1 MONTH - 1 day' AS DATE) AS dernier_jour_mois_derniere_declaration_contrat,
                public.contrats.date_fin_previsionnelle,
                public.etablissements.date_derniere_declaration AS date_derniere_declaration_etablissement,
                public.etablissements.ouvert AS ouverture_etablissement
            FROM public.contrats
            INNER JOIN public.etablissements
                ON public.contrats.etablissement_id = public.etablissements.etablissement_id
        ) AS dernieres_declarations
        WHERE dernieres_declarations.contrat_id = public.contrats.contrat_id
            AND (dernieres_declarations.date_derniere_declaration_etablissement >= (public.contrats.date_derniere_declaration + INTERVAL '3 MONTH') OR dernieres_declarations.ouverture_etablissement IS FALSE)
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);

        -- REMOVE INFERED END DATES WITH NEW DECLARATIONS
        UPDATE public.contrats
        SET date_fin_effective = NULL,
            statut_fin = NULL
        FROM (
            SELECT
                public.contrats.contrat_id,
                public.contrats.date_derniere_declaration AS date_derniere_declaration_contrat,
                public.etablissements.date_derniere_declaration AS date_derniere_declaration_etablissement,
                public.etablissements.ouvert AS ouverture_etablissement
            FROM public.contrats
            INNER JOIN public.etablissements
                ON public.contrats.etablissement_id = public.etablissements.etablissement_id
        ) AS dernieres_declarations
        WHERE dernieres_declarations.contrat_id = public.contrats.contrat_id
            AND ouverture_etablissement IS TRUE
            AND dernieres_declarations.date_derniere_declaration_etablissement < (public.contrats.date_derniere_declaration + INTERVAL '3 MONTH')
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);

        -- INFER END DATES IN CASE OF EMPLOYER CHANGES FOR OLD EMPLOYER'S CONTRATS
        -- get contrats which has been transfered (info in changements_contrats)
        WITH contrats_transferes AS (
            SELECT
                CAST(changements_contrats.siret_ancien_employeur AS BIGINT) AS siret_ancien_employeur,
                CONCAT(
                    COALESCE(changements_contrats.ancien_numero, contrat_new.numero),
                    '_',
                    CAST(COALESCE(changements_contrats.ancienne_date_debut, contrat_new.date_debut) AS CHAR(10))
                ) AS ancienne_contrat_key,
                salarie_new.salarie_key AS ancienne_salarie_key,
                changements_contrats.date_modification AS date_modification
            FROM source.source_changements_contrats AS changements_contrats
            INNER JOIN source.link_contrats AS link
                ON changements_contrats.source_contrat_id = link.source_contrat_id
            INNER JOIN public.contrats AS contrat_new
                ON contrat_new.contrat_id = link.contrat_id
            INNER JOIN public.salaries AS salarie_new
                ON salarie_new.salarie_id = contrat_new.salarie_id
            INNER JOIN public.etablissements AS etablissement_new
                ON contrat_new.etablissement_id = etablissement_new.etablissement_id
            WHERE changements_contrats.siret_ancien_employeur IS NOT NULL
                AND etablissement_new.etablissement_key != changements_contrats.siret_ancien_employeur
        ),

        -- infer end dates for contrats in old employer
        contrats_a_fermer AS (
            SELECT
                contrat_old.contrat_id,
                contrats_transferes.date_modification
            FROM contrats_transferes
            INNER JOIN public.etablissements AS etablissement_old
                ON etablissement_old.etablissement_key = contrats_transferes.siret_ancien_employeur
            INNER JOIN public.salaries AS salarie_old
                ON salarie_old.etablissement_id = etablissement_old.etablissement_id
                AND salarie_old.salarie_key = contrats_transferes.ancienne_salarie_key
            INNER JOIN public.contrats AS contrat_old
                ON contrat_old.salarie_id = salarie_old.salarie_id
                AND contrat_old.contrat_key = contrats_transferes.ancienne_contrat_key
        )

        -- set date_fin_effective at the date of employer changer
        UPDATE public.contrats
        SET date_fin_effective = date_modification - INTERVAL '1 day',
            statut_fin = 2
        FROM contrats_a_fermer
        WHERE contrats_a_fermer.contrat_id = public.contrats.contrat_id
            AND (public.contrats.statut_fin IS NULL OR public.contrats.statut_fin = 1);
        """

        write_sql_file("monthly_integration", "modify_fins_contrats", query, True)

    if generate_all or args.file == "create_expected_tables":
        query = """
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
        """

        write_sql_file("test_integration", "create_expected_tables", query, True)

    if generate_all or args.file == "load_expected_entreprises":
        query = get_copy_query("test.expected_entreprises", source=True)

        write_sql_file("test_integration", "load_expected_entreprises", query, True)

    if generate_all or args.file == "load_expected_etablissements":
        query = get_copy_query("test.expected_etablissements", source=True)

        write_sql_file("test_integration", "load_expected_etablissements", query, True)

    if generate_all or args.file == "load_expected_salaries":
        query = get_copy_query("test.expected_salaries", source=True)

        write_sql_file("test_integration", "load_expected_salaries", query, True)

    if generate_all or args.file == "load_expected_contrats":
        query = get_copy_query("test.expected_contrats", source=True)

        write_sql_file("test_integration", "load_expected_contrats", query, True)

    if generate_all or args.file == "load_expected_postes":
        query = get_copy_query("test.expected_postes", source=True)

        write_sql_file("test_integration", "load_expected_postes", query, True)

    if generate_all or args.file == "load_expected_activites":
        query = get_copy_query("test.expected_activites", source=True)

        write_sql_file("test_integration", "load_expected_activites", query, True)

    if generate_all or args.file == "load_expected_contrats_comparisons":
        query = get_copy_query("test.expected_contrats_comparisons", source=True)

        write_sql_file(
            "test_integration", "load_expected_contrats_comparisons", query, True
        )

    if generate_all or args.file == "load_mock_entreprises":
        query = get_copy_query("public.entreprises", source=True)

        write_sql_file("mock_integration", "load_mock_entreprises", query, True)

    if generate_all or args.file == "load_mock_etablissements":
        query = get_copy_query("public.etablissements", source=True)

        write_sql_file("mock_integration", "load_mock_etablissements", query, True)

    if generate_all or args.file == "load_mock_salaries":
        query = get_copy_query("public.salaries", source=True)

        write_sql_file("mock_integration", "load_mock_salaries", query, True)

    if generate_all or args.file == "load_mock_contrats":
        query = get_copy_query("public.contrats", source=True)

        write_sql_file("mock_integration", "load_mock_contrats", query, True)

    if generate_all or args.file == "load_mock_postes":
        query = get_copy_query("public.postes", source=True)

        write_sql_file("mock_integration", "load_mock_postes", query, True)

    if generate_all or args.file == "load_mock_activites":
        query = get_copy_query("public.activites", source=True)

        write_sql_file("mock_integration", "load_mock_activites", query, True)

    if generate_all or args.file == "monthly_sanity_checks":
        queries = []
        queries.append(
            """
        DO $$
        DECLARE
        do_trinomial_indexes_are_consistent integer;
        BEGIN
            SELECT COUNT(*)
            INTO do_trinomial_indexes_are_consistent
            FROM public.contrats AS c
            INNER JOIN public.salaries AS s
                ON c.salarie_id = s.salarie_id
            WHERE c.etablissement_id != s.etablissement_id;

        ASSERT do_trinomial_indexes_are_consistent = 0, 'Inconsistance in trinomial indexes.\n \
        One contrat refers to a salarie which does not have the same etablissement_id.';
        END$$;
        """
        )

        queries.append(
            check_id_consistency_in_link_table(
                "source.link_entreprises", "public.entreprises", "entreprise_id"
            )
        )
        queries.append(
            check_id_consistency_in_link_table(
                "source.link_etablissements",
                "public.etablissements",
                "etablissement_id",
            )
        )
        queries.append(
            check_id_consistency_in_link_table(
                "source.link_salaries", "public.salaries", "salarie_id"
            )
        )
        queries.append(
            check_id_consistency_in_link_table(
                "source.link_contrats", "public.contrats", "contrat_id"
            )
        )
        query = combine_queries(queries)

        write_sql_file("monthly_integration", "monthly_sanity_checks", query, True)

    if generate_all or args.file == "load_postes":
        query = """
        WITH source_postes AS (
            SELECT
                UPPER(TRIM(libelleemploi)) AS libelle,
                MAX(datedeclaration) AS date_derniere_declaration
            FROM raw.raw_contrats
            GROUP BY UPPER(TRIM(libelleemploi))
        )

        INSERT INTO public.postes (
            poste_key,
            date_derniere_declaration
        )
        SELECT
            libelle,
            date_derniere_declaration
        FROM source_postes
        ON CONFLICT ON CONSTRAINT uk_poste
        DO UPDATE SET date_derniere_declaration = excluded.date_derniere_declaration;
        """

        write_sql_file("monthly_integration", "load_postes", query, True)

    if generate_all or args.file == "set_dernier_mois_de_declaration_integre":
        query = """
        UPDATE public.chargement_donnees
        SET dernier_mois_de_declaration_integre = '{{ params.filedate }}';
        """

        write_sql_file(
            "monthly_integration",
            "set_dernier_mois_de_declaration_integre",
            query,
            True,
        )

    if generate_all or args.file == "extract_activites":
        query = get_copy_query("raw.raw_activites", source=False)

        write_sql_file("monthly_integration", "extract_activites", query, True)

    if generate_all or args.file == "extract_remunerations":
        query = get_copy_query("raw.raw_remunerations", source=False)

        write_sql_file("monthly_integration", "extract_remunerations", query, True)

    if generate_all or args.file == "extract_versements":
        query = get_copy_query("raw.raw_versements", source=False)

        write_sql_file("monthly_integration", "extract_versements", query, True)

    if generate_all or args.file == "transform_activites":
        query = """
        TRUNCATE TABLE source.source_activites CASCADE;
        INSERT INTO source.source_activites (
            source_remuneration_id,
            source_activite_id,
            source_contrat_id,
            date_debut_paie,
            date_fin_paie,
            type_heures,
            nombre_heures,
            date_derniere_declaration
        )
        SELECT
            raw.raw_remunerations.idremuneration AS source_remuneration_id,
            raw.raw_activites.idactivite AS source_activite_id,
            raw.raw_contrats.idcontrat AS source_contrat_id,
            raw.raw_remunerations.datedebutperiodepaie AS date_debut_paie,
            raw.raw_remunerations.datefinperiodepaie AS date_fin_paie,
            CASE
                -- heures standards rémunérées
                WHEN raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.typeactivite = '01' THEN 1
                -- heures non rémunérées
                WHEN raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.typeactivite = '02' THEN 2
                -- heures supplémentaires rémunérées
                WHEN raw.raw_remunerations.typeremuneration IN ('017', '018') THEN 3
            END AS type_heures,
            CASE
                WHEN raw.raw_remunerations.typeremuneration = '002' THEN
                    CASE
                        WHEN raw.raw_activites.unitemesure IN ('10', '21') THEN raw.raw_activites.mesure
                        WHEN raw.raw_activites.unitemesure IN ('12', '20') THEN raw.raw_activites.mesure * 7
                        WHEN raw.raw_activites.unitemesure IS NULL AND raw.raw_contrats.codeunitequotite IN ('10', '21') THEN raw.raw_activites.mesure
                        WHEN raw.raw_activites.unitemesure IS NULL AND raw.raw_contrats.codeunitequotite IN ('12', '20') THEN raw.raw_activites.mesure * 7
                        ELSE 0
                    END
                WHEN raw.raw_remunerations.typeremuneration IN ('017', '018') THEN COALESCE(raw.raw_remunerations.nombreheure, 0)
            END AS nombre_heures,
            raw.raw_remunerations.datedeclaration AS date_derniere_declaration
        FROM raw.raw_remunerations
        LEFT JOIN raw.raw_activites
            ON raw.raw_remunerations.idremuneration = raw.raw_activites.idremuneration
        INNER JOIN raw.raw_versements
            ON raw.raw_versements.idversement = raw.raw_remunerations.idversement
        INNER JOIN raw.raw_contrats
            ON raw.raw_contrats.idindividu = raw.raw_versements.idindividu
            AND raw.raw_contrats.numero = raw.raw_remunerations.numerocontrat
        WHERE (raw.raw_remunerations.typeremuneration = '002' AND raw.raw_activites.idactivite IS NOT NULL)
            OR raw.raw_remunerations.typeremuneration IN ('017', '018');
        """

        write_sql_file("monthly_integration", "transform_activites", query, True)

    if generate_all or args.file == "load_activites":
        query = """
        WITH unique_periods AS (
            SELECT
                date_debut_paie,
                date_fin_paie
            FROM source.source_activites
            WHERE date_fin_paie >= date_debut_paie
            GROUP BY (date_debut_paie, date_fin_paie)
        )
        , one_row_per_month AS (
            SELECT
                GENERATE_SERIES(DATE_TRUNC('month', date_debut_paie)::DATE,
                                DATE_TRUNC('month', date_fin_paie)::DATE,
                                '1 month'::INTERVAL)::DATE AS mois,
                date_debut_paie,
                date_fin_paie
            FROM unique_periods
        )
        , monthly_cut AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                GREATEST(date_debut_paie, mois) AS monthly_debut_periode_paie,
                LEAST(date_fin_paie, (mois + INTERVAL '1 MONTH - 1 DAY')::DATE) AS monthly_fin_periode_paie
            FROM one_row_per_month
        )
        , monthly_nb AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                COUNT(CASE WHEN weekday_number NOT IN (0,6) AND public_holiday IS NOT TRUE THEN 1 END) AS nb_working_days
            FROM monthly_cut
            INNER JOIN public.daily_calendar
                ON monthly_cut.monthly_debut_periode_paie <= public.daily_calendar.full_date
                AND monthly_cut.monthly_fin_periode_paie >= public.daily_calendar.full_date
            GROUP BY (date_debut_paie, date_fin_paie, mois)
        )
        , monhtly_and_total_nb AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                nb_working_days,
                SUM(nb_working_days) OVER (PARTITION BY date_debut_paie, date_fin_paie) AS div_working_days
            FROM monthly_nb
        )
        , monthly_ratio AS (
            SELECT
                mois,
                date_debut_paie,
                date_fin_paie,
                CASE WHEN div_working_days != 0 THEN nb_working_days / div_working_days ELSE 1 END AS ratio
            FROM monhtly_and_total_nb
        )
        , linked_activites AS (
            SELECT
                source.link_contrats.contrat_id,
                date_debut_paie,
                date_fin_paie,
                type_heures,
                nombre_heures,
                date_derniere_declaration
            FROM source.source_activites
            INNER JOIN source.link_contrats
                ON source.link_contrats.source_contrat_id = source.source_activites.source_contrat_id
        )
        , new_activites AS (
            SELECT
                linked_activites.contrat_id,
                monthly_ratio.mois,
                SUM(CASE WHEN linked_activites.type_heures = 1 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_standards_remunerees,
                SUM(CASE WHEN linked_activites.type_heures = 2 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_non_remunerees,
                SUM(CASE WHEN linked_activites.type_heures = 3 THEN linked_activites.nombre_heures * monthly_ratio.ratio ELSE 0 END) AS heures_supp_remunerees,
                MAX(linked_activites.date_derniere_declaration) AS date_derniere_declaration
            FROM linked_activites
            INNER JOIN monthly_ratio
                ON linked_activites.date_debut_paie = monthly_ratio.date_debut_paie
                AND linked_activites.date_fin_paie = monthly_ratio.date_fin_paie
            GROUP BY (linked_activites.contrat_id, monthly_ratio.mois)
        )
        MERGE INTO public.activites
        USING new_activites
            ON new_activites.contrat_id = public.activites.contrat_id
            AND new_activites.mois = public.activites.mois
        WHEN NOT MATCHED THEN
            INSERT (
                contrat_id,
                mois,
                heures_standards_remunerees,
                heures_non_remunerees,
                heures_supp_remunerees,
                date_derniere_declaration
            )
            VALUES (
                new_activites.contrat_id,
                new_activites.mois,
                new_activites.heures_standards_remunerees,
                new_activites.heures_non_remunerees,
                new_activites.heures_supp_remunerees,
                new_activites.date_derniere_declaration
            )
        WHEN MATCHED THEN
        UPDATE SET
        heures_standards_remunerees = public.activites.heures_standards_remunerees + new_activites.heures_standards_remunerees,
        heures_non_remunerees = public.activites.heures_non_remunerees + new_activites.heures_non_remunerees,
        heures_supp_remunerees = public.activites.heures_supp_remunerees + new_activites.heures_supp_remunerees,
        date_derniere_declaration = GREATEST(public.activites.date_derniere_declaration, new_activites.date_derniere_declaration);
        """

        write_sql_file("monthly_integration", "load_activites", query, True)

    if generate_all or args.file == "update_natures_contrats":
        query = update_static_table_query("natures_contrats", "code_nature_contrat")

        write_sql_file("update_database", "update_natures_contrats", query, True)

    if generate_all or args.file == "update_motifs_recours":
        query = update_static_table_query("motifs_recours", "code_motif_recours")

        write_sql_file("update_database", "update_motifs_recours", query, True)

    if generate_all or args.file == "update_naf":
        query = update_static_table_query("naf", "code_naf")

        write_sql_file("update_database", "update_naf", query, True)

    if generate_all or args.file == "update_conventions_collectives":
        query = update_static_table_query(
            "conventions_collectives", "code_convention_collective"
        )

        write_sql_file("update_database", "update_conventions_collectives", query, True)

    if generate_all or args.file == "update_categories_juridiques_insee":
        query = update_static_table_query(
            "categories_juridiques_insee", "code_categorie_juridique_insee"
        )

        write_sql_file(
            "update_database", "update_categories_juridiques_insee", query, True
        )

    if generate_all or args.file == "modify_ouverture_etablissements":
        query = """
        -- Fermeture des etablissements n'ayant pas fait l'objet de déclarations pendant 3 mois
        UPDATE public.etablissements
        SET ouvert = FALSE
        FROM public.chargement_donnees
        WHERE public.chargement_donnees.dernier_mois_de_declaration_integre >= (public.etablissements.date_derniere_declaration + INTERVAL '3 MONTH');

        -- Ré-ouverture des établissements ayant une déclaration de moins de 3 mois
        UPDATE public.etablissements
        SET ouvert = TRUE
        FROM public.chargement_donnees
        WHERE public.chargement_donnees.dernier_mois_de_declaration_integre < (public.etablissements.date_derniere_declaration + INTERVAL '3 MONTH');
        """

        write_sql_file(
            "monthly_integration", "modify_ouverture_etablissements", query, True
        )

    if generate_all or args.file == "remove_old_data":
        queries = []
        queries.append(
            remove_old_data(
                table_name="entreprises", retention_period=base.DATA_RETENTION_PERIOD
            )
        )
        queries.append(
            remove_old_data(
                table_name="etablissements", retention_period=base.DATA_RETENTION_PERIOD
            )
        )
        queries.append(
            remove_old_data(
                table_name="salaries", retention_period=base.DATA_RETENTION_PERIOD
            )
        )
        queries.append(
            remove_old_data(
                table_name="contrats", retention_period=base.DATA_RETENTION_PERIOD
            )
        )
        queries.append(
            remove_old_data(
                table_name="postes", retention_period=base.DATA_RETENTION_PERIOD
            )
        )
        query = combine_queries(queries=queries)

        write_sql_file("monthly_integration", "remove_old_data", query, True)

    if generate_all or args.file == "reindex_tables":
        query = """
        REINDEX TABLE contrats;
        REINDEX TABLE salaries;
        """

        write_sql_file("monthly_integration", "reindex_tables", query, True)

    if generate_all or args.file == "update_holidays":
        query = []
        query.append(
            """
        DROP TABLE IF EXISTS public.holidays_calendar CASCADE;
        CREATE TABLE public.holidays_calendar (
            holiday_date DATE NOT NULL
        );
            """
        )
        query.append(get_copy_query(table_name="public.holidays_calendar", source=True))
        query.append(
            """
        UPDATE public.daily_calendar
        SET public_holiday = COALESCE((EXISTS (SELECT 1 FROM public.holidays_calendar WHERE holiday_date = full_date) OR public_holiday IS TRUE), FALSE);
            """
        )
        query.append(
            """
        DROP TABLE public.holidays_calendar;
            """
        )
        query = combine_queries(query)

        write_sql_file("update_database", "update_holidays", query, True)

    if generate_all or args.file == "update_zonage":
        query = []
        query.append(get_copy_query(table_name="raw.raw_zonage", source=True))
        query.append(
            """
        TRUNCATE TABLE public.zonage;
        INSERT INTO public.zonage(
            siret,
            unite_controle
        )
        SELECT
            CAST(TRIM(siret) AS BIGINT) AS siret,
            CAST(unite_controle AS CHAR(6)) AS unite_controle
        FROM raw.raw_zonage;
            """
        )

        query = combine_queries(query)

        write_sql_file("update_database", "update_zonage", query, True)

    if generate_all or args.file == "integration_log_begin":
        query = """
        INSERT INTO log.integrations_logs(declaration_date, stage_type, stage_time) VALUES ('{{ params.filedate }}', 'BEGIN', CLOCK_TIMESTAMP());
        """
        write_sql_file("monthly_integration", "integration_log_begin", query)

    if generate_all or args.file == "integration_log_end":
        query = """
        INSERT INTO log.integrations_logs(declaration_date, stage_type, stage_time) VALUES ('{{ params.filedate }}', 'END', CLOCK_TIMESTAMP());
        """
        write_sql_file("monthly_integration", "integration_log_end", query)

    if generate_all or args.file == "create_dag_status_functions":
        query = """
        CREATE OR REPLACE FUNCTION SET_STATUS_TO_ONGOING()
        RETURNS VOID AS $$
        BEGIN
            IF (SELECT status <> 'SUCCESS' FROM sys.current_status) THEN
                RAISE EXCEPTION 'Last DAG let the database in non SUCCESS status. If you want to force execution, please run : UPDATE sys.current_status SET status = ''SUCCESS''.';
            ELSE
                UPDATE sys.current_status SET status = 'ONGOING';
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION SET_STATUS_TO_SUCCESS()
        RETURNS VOID AS $$
        BEGIN
            UPDATE sys.current_status SET status = 'SUCCESS';
        END;
        $$ LANGUAGE plpgsql;
        """
        write_sql_file("init_database", "create_dag_status_functions", query)

    if generate_all or args.file == "create_anonymous_schema":
        queries = [
            """
        DROP SCHEMA IF EXISTS anonymous CASCADE;
        CREATE SCHEMA anonymous;

        CREATE TABLE anonymous.selection_etablissements (
            etablissement_key BIGINT NOT NULL,
            comment VARCHAR(100)
        );
        """
        ]

        queries.append(
            get_copy_query(
                "anonymous.selection_etablissements",
                source=True,
                freeze=True,
                encoding="UTF8",
            )
        )

        queries.append(
            """
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
        """
        )

        queries.append(
            """
        CREATE TABLE anonymous.chargement_donnees AS TABLE public.chargement_donnees;
        CREATE TABLE anonymous.categories_juridiques_insee AS TABLE public.categories_juridiques_insee;
        CREATE TABLE anonymous.conventions_collectives AS TABLE public.conventions_collectives;
        CREATE TABLE anonymous.motifs_recours AS TABLE public.motifs_recours;
        CREATE TABLE anonymous.natures_contrats AS TABLE public.natures_contrats;
        CREATE TABLE anonymous.naf AS TABLE public.naf;
        -- CREATE TABLE anonymous.zonage AS TABLE public.zonage;
        CREATE TABLE anonymous.daily_calendar AS TABLE public.daily_calendar;
        """
        )

        queries.append(
            shift_anonymization(
                "etablissements",
                "etablissement_id",
                "etablissements.etablissement_id IN (SELECT etablissement_id FROM anonymous.selection_etablissements)",
                [
                    ("etablissement_id", ""),
                    ("entreprise_id", ""),
                    ("etablissement_key", "twin"),
                    ("siege_social", ""),
                    ("ouvert", ""),
                    ("enseigne", "twin"),
                    ("adresse", "twin"),
                    ("complement_adresse", "twin"),
                    ("code_postal", ""),
                    ("commune", ""),
                    ("code_categorie_juridique_insee", ""),
                    ("code_naf", ""),
                    ("code_convention_collective", ""),
                    ("date_premiere_declaration", ""),
                    ("date_derniere_declaration", ""),
                ],
            )
        )

        queries.append(
            shift_anonymization(
                "entreprises",
                "entreprise_id",
                "entreprises.entreprise_id IN (SELECT entreprise_id FROM anonymous.etablissements)",
                [
                    ("entreprise_id", ""),
                    ("entreprise_key", "twin"),
                    ("raison_sociale", "twin"),
                    ("date_premiere_declaration", ""),
                    ("date_derniere_declaration", ""),
                ],
            )
        )

        queries.append(
            shift_anonymization(
                "salaries",
                "salarie_id",
                "salaries.etablissement_id IN (SELECT etablissement_id FROM anonymous.etablissements) AND salaries.nir IS NOT NULL",
                [
                    ("salarie_id", ""),
                    ("etablissement_id", ""),
                    ("salarie_key", ""),
                    ("nir", ""),
                    ("nom_famille", "twin"),
                    ("nom_usage", "twin"),
                    ("prenoms", "twin"),
                    ("date_naissance", "twin"),
                    ("lieu_naissance", "twin"),
                    ("allocated_from_ett", ""),
                    ("date_derniere_declaration", ""),
                ],
            )
        )

        queries.append(
            """
        CREATE TABLE anonymous.contrats AS
        SELECT contrats.*
        FROM public.contrats
        INNER JOIN anonymous.etablissements
            ON etablissements.etablissement_id = contrats.etablissement_id;
        """
        )

        queries.append(
            """
        CREATE TABLE anonymous.postes AS
        SELECT DISTINCT postes.*
        FROM public.postes
        INNER JOIN anonymous.contrats
            ON contrats.poste_id = postes.poste_id;
        """
        )

        queries.append(
            """
        CREATE TABLE anonymous.activites AS
        SELECT activites.*
        FROM public.activites
        INNER JOIN anonymous.contrats
            ON COALESCE(contrats.ett_contrat_id, contrats.contrat_id) = activites.contrat_id;
        """
        )

        query = combine_queries(queries)
        write_sql_file("update_database", "create_anonymous_schema", query, True)

    if generate_all or args.file == "clean_database":
        query = """
        DO $$ 
        DECLARE 
            table_name text; 
        BEGIN 
            FOR table_name IN (SELECT tables.table_name FROM information_schema.tables WHERE table_schema = 'raw') 
            LOOP 
                EXECUTE 'TRUNCATE TABLE raw.' || table_name || ' CASCADE'; 
            END LOOP; 
        END $$;

        DO $$ 
        DECLARE 
            table_name text; 
        BEGIN 
            FOR table_name IN (SELECT tables.table_name FROM information_schema.tables WHERE table_schema = 'source') 
            LOOP 
                EXECUTE 'TRUNCATE TABLE source.' || table_name || ' CASCADE'; 
            END LOOP; 
        END $$;
        """
        write_sql_file("monthly_integration", "clean_database", query, True)
