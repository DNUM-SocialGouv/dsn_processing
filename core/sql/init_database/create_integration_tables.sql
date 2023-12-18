


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
        

        DROP TABLE IF EXISTS source.map_changes_salaries;
        CREATE TABLE source.map_changes_salaries (
            old_salarie_id BIGINT,
            new_salarie_id BIGINT,
            date_modification DATE
        );
        

        DROP TABLE IF EXISTS source.map_changes_salaries_impact_contrats;
        CREATE TABLE source.map_changes_salaries_impact_contrats (
            old_contrat_id BIGINT,
            new_contrat_id BIGINT,
            date_modification DATE
        );
        

        DROP TABLE IF EXISTS source.map_changes_contrats;
        CREATE TABLE source.map_changes_contrats (
            old_contrat_id BIGINT,
            new_contrat_id BIGINT,
            date_modification DATE
        );
        