# Import et accès aux données source

## Fichiers des données brutes de la DSN

Les fichiers bruts sont issus de la base DSNP3.

### Modalités

La DSNP3 est une base SQLServer de données DSN.

L'import des données DSNP3 vers Champollion a été implémenté par [VDDAL](https://msociauxfr.sharepoint.com/:x:/r/teams/EIG71/Documents%20partages/General/Commun/Documentation/contacts.xlsx?d=w86d385866e8f4768b018b6e069018b5e&csf=1&web=1&e=lKg5GS). L'import mensuel vers la VM de stockage de Champollion n'est pas automatisé, cela nécessite des opérations manuelles chaque mois. Cette dernière est détaillée dans le fichier [Base Champollion/Données/Alimentation données DSN/procedure_import_DSN.docx](https://msociauxfr.sharepoint.com/:w:/r/teams/EIG71/Documents%20partages/General/Base%20Champollion/Donn%C3%A9es/Alimentation%20donn%C3%A9es%20DSN/procedure_import_DSN.docx?d=wfdb90b3855024a20b736be1b090637a1&csf=1&web=1&e=PCMdyt) de l'espace Teams Champollion.

Le logiciel utilisé pour l'import est SSIS. Les caractérisques décrites dans la suite de cette documentation sont donc configurées dans la solution SSIS.

Le compte utilisé pour transférer les fichiers sur la VM de stockage de Champollion est le compte de service `svc.filer`. La connexion à ce compte s'effectue par clef SSH qu'il faut demander à l'infogérant (en l'occurrence [OBSQN](https://msociauxfr.sharepoint.com/:x:/r/teams/EIG71/Documents%20partages/General/Commun/Documentation/contacts.xlsx?d=w86d385866e8f4768b018b6e069018b5e&csf=1&web=1&e=lKg5GS)).

### Scripts de sélection des données

Pour chaque table souhaitée de la DSNP3, un script SQL permet la récupération des données chargées en base sur la période qui nous intéresse (voir section *Chronologie* ci-dessous). Chaque script est formatté de la sorte : 

```SQL
USE [DSNP3]
GO

SELECT 
       [dbo].[NomTable].[PremiereColonneSouhaitee]

      , ...

      ,[dbo].[NomTable].[DerniereColonneSouhaitee]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[NomTable]
  INNER JOIN [dbo].[TableParent]
	ON [dbo].[NomTable].IdParentColonne = [dbo].[TableParent].IdColonne

  ... (autres jointures si nécessaires) ...

  INNER JOIN [dbo].[Declaration]
	ON [dbo].[TableParent].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
```

Pour la clause de sélection temporelle, on utilise la date de chargement (sous format id) de la table la plus *haute*, c'est-à-dire la table `Declaration`, afin de minimiser le risque de dates de chargement erronées (faille issue de la procédure d'intégration des données en DSNP3).

Les scripts sont stockés dans `core/python/raw_files_management/raw_DSN_selection_scripts/`.

### Format des fichiers de données

Pour être traités par la pipeline, les fichiers bruts de la DSNP3 (un par table) doivent : 

- être contenus dans une archive compressée au format `7z` sécurisée par un mot de passe (`${WORKFLOW_ARCHIVES_PASSWORD}`) et intitulée `champollion_YYYYMM01.7z` avec `YYYYMM01` le mois de déclaration ;
- être intitulés `champollion_<nom de la table>_YYYY-MM-01.csv` ;
- être au format `CSV`, avec une première ligne correspondant aux noms des colonnes et un encodage `WIN1252` (aussi appelé `cp1252`) ;
- avoir le symbole `|` comme délimiteur de texte et le symbole `;` comme séparateur ;
- respecter l'ordre des colonnes tel que défini ci-après.

Les fichiers contenus dans l'archive sont les suivants :

```
champollion_YYYYMM01.7z  
└─── champollion_activite_YYYY-MM-01.csv 
└─── champollion_anciennete_YYYY-MM-01.csv
└─── champollion_arretdetravail_YYYY-MM-01.csv
└─── champollion_autresuspensionexecutioncontrat_YYYY-MM-01.csv
└─── champollion_contrat_YYYY-MM-01.csv
└─── champollion_contratchangement_YYYY-MM-01.csv
└─── champollion_contratfin_YYYY-MM-01.csv
└─── champollion_declaration_YYYY-MM-01.csv
└─── champollion_etablissement_YYYY-MM-01.csv
└─── champollion_individu_YYYY-MM-01.csv
└─── champollion_individuchangement_YYYY-MM-01.csv
└─── champollion_lieutravail_YYYY-MM-01.csv
└─── champollion_penebilite_YYYY-MM-01.csv
└─── champollion_remuneration_YYYY-MM-01.csv
└─── champollion_tempspartieltherapeutique_YYYY-MM-01.csv
└─── champollion_versement_YYYY-MM-01.csv
```

#### Ordre des colonnes

<details>
  <summary>Afficher la liste</summary>

```json
{
    "champollion_activite": [
        "IdActivite",
        "IdRemuneration",
        "TypeActivite",
        "Mesure",
        "UniteMesure",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_anciennete": [
        "IdAnciennete",
        "IdIndividu",
        "TypeAnciennete",
        "UniteMesure",
        "Valeur",
        "NumeroContrat",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_arretdetravail": [
        "IdArretDeTravail",
        "IdContrat",
        "MotifArret",
        "DateDernierJourTravaille",
        "DateFinPrevisionelle",
        "DateReprise",
        "MotifReprise",
        "DateChargement",
        "IdDateChargement",
        "Subrogation",
        "DateDebutSubrogation",
        "DateFinSubrogation",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_autresuspensionexecutioncontrat": [
        "IdAutreSuspensionExecutionContrat",
        "IdContrat",
        "MotifSuspension",
        "DateDebutSuspension",
        "DateFinSuspension",
        "DateChargement",
        "IdDateChargement",
        "PositionDetachement",
        "NombreJourOSuspenFractio",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_contrat": [
        "IdContrat",
        "IdIndividu",
        "DateDebut",
        "CodeStatutConv",
        "CodeStatutCatRetraiteCompl",
        "CodePcsEse",
        "CodeCompltPcsEse",
        "LibelleEmploi",
        "CodeNature",
        "CodeDispPolitiquePublique",
        "Numero",
        "DateFinPrev",
        "CodeUniteQuotite",
        "QuotiteTravailCategorie",
        "QuotiteTravailContrat",
        "ModaliteExerciceTempsTravail",
        "ComplementBaseRegimeObligatoire",
        "CodeCCN",
        "CodeRegimeMaladie",
        "LieuTravail",
        "CodeRegimeRisqueVieillesse",
        "CodeMotifRecours",
        "CodeCaisseCP",
        "TravailleurAEtrangerSS",
        "CodeMotifExclusion",
        "CodeStatutEmploi",
        "CodeDelegataireRisqueMaladie",
        "CodeEmploiMultiple",
        "CodeEmployeurMultiple",
        "CodeMetier",
        "CodeRegimeRisqueAT",
        "CodeRisqueAccidentTravail",
        "PositionConventionCollective",
        "CodeStatutCatAPECITA",
        "SalarieTpsPartielCotisTpsPlein",
        "RemunerationPourboire",
        "SIRETEtabUtilisateur",
        "FPCodeComplPcsEse",
        "FPNaturePoste",
        "FPQuotiteTravailTempsComplet",
        "TauxTravailTempsPartiel",
        "CodeCatService",
        "FPIndiceBrut",
        "FPIndiceMajore",
        "FPNBI",
        "FPIndiceBrutOrigine",
        "FPIndiceBrutCotiEmploiS",
        "FPAncEmployPublic",
        "FPMaintienTraitContractuelT",
        "FPTypeDetachement",
        "TauxServiceActif",
        "NiveauRemuneration",
        "Echelon",
        "CoefficientHierarchique",
        "StatutBOETH",
        "CompltDispositifPublic",
        "MiseDispoExterneIndividu",
        "CatClassementfinale",
        "CollegeCNIEG",
        "AmenagTpsTravActivParti",
        "DateChargement",
        "IdDateChargement",
        "TauxDeductForfFraisPro",
        "NumeroInterneEPublic",
        "TypeGestionAC",
        "DateAdhesion",
        "CodeAffAssChomage",
        "StatutOrgSpectacle",
        "GenreNavigation",
        "Grade",
        "FPIndiceCTI",
        "FINESSGeographique",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_contratchangement": [
        "IdContratChangement",
        "IdContrat",
        "DateModification",
        "CodeStatutConv",
        "CodeStatutCatRetraiteCompl",
        "CodeNature",
        "CodeDispPolitiquePublique",
        "CodeUniteQuotite",
        "QuotiteTravailContrat",
        "ModaliteExercieTempsTravail",
        "ComplementBaseRegimeObligatoire",
        "CodeCCN",
        "SiretEtab",
        "LieuTravail",
        "Numero",
        "CodeMotifRecours",
        "TravailleurAEtrangerSS",
        "CodePcsEse",
        "CodeCompltPcsEse",
        "DateDebut",
        "QuotiteTravailCategorie",
        "CodeCaisseCP",
        "CodeRisque",
        "CodeStatutCatAPECITA",
        "SalarieTpsPartielCotisTpsPlein",
        "ProfondeurRecalculPaie",
        "FPCodeComplPcsEse",
        "FPNaturePoste",
        "FPQuotiteTravailTempsComplet",
        "TauxTravailTempsPartiel",
        "CodeCatService",
        "FPIndiceBrut",
        "FPIndiceMajore",
        "FPNBI",
        "FPIndiceBrutOrigine",
        "FPIndiceBrutCotiEmploiS",
        "FPAncEmployPublic",
        "FPMaintienTraitContractuelT",
        "TauxServiceActif",
        "NiveauRemuneration",
        "Echelon",
        "CoefficientHierarchique",
        "StatutBOETH",
        "CompltDispositifPublic",
        "MiseDispoExterneIndividu",
        "CatClassementfinale",
        "CollegeCNIEG",
        "AmenagTpsTravActivParti",
        "FPTypeDetachement",
        "DateChargement",
        "IdDateChargement",
        "TauxDeductForfFraisPro",
        "CodeRegimeMaladie",
        "CodeRegimeRisqueVieillesse",
        "PositionConventionCollective",
        "CodeRisqueAccidentTravail",
        "CodeStatutEmploi",
        "CodeEmploiMultiple",
        "CodeEmployeurMultiple",
        "Grade",
        "FPIndiceCTI",
        "FINESSGeographique",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_contratfin": [
        "IdContratFin",
        "IdContrat",
        "DateFin",
        "CodeMotifRupture",
        "DernierJourTrav",
        "DeclarationFinContratUsage",
        "SoldeCongesAcqNonPris",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_declaration": [
        "IdDeclaration",
        "IdEnvoi",
        "NatureDeclaration",
        "TypeDeclaration",
        "NumeroFraction",
        "NumeroOrdreDeclaration",
        "DateDeclaration",
        "DateFichier",
        "ChampDeclaration",
        "IdentifiantMetier",
        "DeviseDeclaration",
        "SiretEtab",
        "DateChargement",
        "IdDateChargement"
    ],
    "champollion_etablissement": [
        "IdEtablissement",
        "IdDeclaration",
        "Siren",
        "NICEntre",
        "CodeAPEN",
        "VoieEntre",
        "CPEntre",
        "LocaliteEntre",
        "CompltDistributionEntre",
        "CompltVoieEntre",
        "EffectifMoyenEntrepriseFinPeriode",
        "CodePaysEntr",
        "ImplantationEntreprise",
        "DateDebPerRef",
        "DateFinPerRef",
        "RaisonSocialeEntr",
        "NIC",
        "CodeAPET",
        "VoieEtab",
        "CP",
        "Localite",
        "CompltDistributionEtab",
        "CompltVoieEtab",
        "EffectifFinPeriode",
        "Codepays",
        "NatureJuridiqueEmployeur",
        "DateClotureExerciceComptable",
        "DateAdhesionTESECEA",
        "DateSortieTESECEA",
        "CodeINSEECommune",
        "DateEcheanceApplique",
        "CategorieJuridique",
        "EnseigneEtablissement",
        "DateChargement",
        "IdDateChargement",
        "CodeConvCollectiveApplic",
        "CodeConvCollectivePrinci",
        "OprateurCompetences",
        "DateDeclaration"
    ],
    "champollion_individu": [
        "IdIndividu",
        "IdEtablissement",
        "NIRDeclare",
        "NomFamille",
        "NomUsage",
        "Prenoms",
        "CodeSexe",
        "DateNaissance",
        "LieuNaissance",
        "Voie",
        "CP",
        "Localite",
        "CodePays",
        "Distribution",
        "CodeUE",
        "CodeDepartNaissance",
        "CodePaysNaissance",
        "CompltLocalisation",
        "CompltVoie",
        "Mel",
        "Matricule",
        "NTT",
        "NombreEnfantcharge",
        "StatutEtrangerFiscal",
        "Embauche",
        "NiveauFormation",
        "NIR",
        "DateSNGI",
        "NomFamilleSNGI",
        "PrenomsSNGI",
        "NomMaritalSNGI",
        "CodeResultSNGI",
        "IndicCertifSNGI",
        "ComNaissSNGI",
        "CodeDeptNaissSNGI",
        "PaysNaissSNGI",
        "DateNIRSNGI",
        "DateDecesSNGI",
        "DateChargement",
        "IdDateChargement",
        "EstCodifie",
        "NiveauDiplome",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_individuchangement": [
        "IdIndividuChangement",
        "IdIndividu",
        "DateModification",
        "AncienNIR",
        "NomFamille",
        "Prenoms",
        "DateNaissance",
        "DateChargement",
        "IdDateChargement",
        "EstCodifie",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_lieutravail": [
        "IdLieuTravail",
        "IdDeclaration",
        "Siret",
        "CodeAPET",
        "Voie",
        "CP",
        "Localite",
        "CodePays",
        "Distribution",
        "CompltConstruction",
        "CompltVoie",
        "CodeNatureJur",
        "CodeINSEECommune",
        "EnseigneEtab",
        "DateChargement",
        "IdDateChargement",
        "DateDeclaration"
    ],
    "champollion_penebilite": [
        "IdPenebilite",
        "IdIndividu",
        "FacteurExposition",
        "NumeroContrat",
        "AnneeRattachement",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_remuneration": [
        "IdRemuneration",
        "IdVersement",
        "DateDebutPeriodePaie",
        "DateFinPeriodePaie",
        "NumeroContrat",
        "TypeRemuneration",
        "NombreHeure",
        "Montant",
        "TauxRemunPositStatutaire",
        "TauxMajorResidentielle",
        "DateChargement",
        "IdDateChargement",
        "TauxRemuCotisee",
        "TauxMajorationExAExE",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_tempspartieltherapeutique": [
        "IdTempsPartielTherapeutique",
        "IdArretDeTravail",
        "DateDebut",
        "DateFin",
        "Montant",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ],
    "champollion_versement": [
        "IdVersement",
        "IdIndividu",
        "DateVersement",
        "RemunarationNetteFiscale",
        "NumeroVersement",
        "MontantNetVerse",
        "TauxPrelevSource",
        "TypeTauxPrelevSource",
        "IdenTauxPrelevSource",
        "MontantPrelevSource",
        "MontantPartNonImpRevenu",
        "MontantAbattBaseFiscale",
        "MontantDiffAssPASEtRNF",
        "DateChargement",
        "IdDateChargement",
        "IdDeclaration",
        "DateDeclaration"
    ]
}
```

</details>

### Planning des imports

<pre>
         mois N              mois N+1           mois N+2
-- || --------------- || --------------- || --------------- || -->
                              <span style="text-decoration: underline;">└──┘</span>              ↑
                         Autour du 20 du      Le 9 du mois N+2,
                         mois N+1, la         la quasi-totalité
                         majorité des         des déclarations
                         déclarations du      du mois N sont
                         mois N sont          chargées et on
                         chargées en DSN.     récupère donc à
                                              ce moment-là les
                                              déclarations chargées
                                              entre le 9 (inclu) 
                                              du mois N+1 et le 9 
                                              (exclu) du mois N+2
                                              ce qui correspond
                                              quasi-exclusivement
                                              aux déclarations du
                                              mois N.
</pre>

L'intérêt de venir *chercher* les déclarations du mois N le 9 du mois N+2, et non le 25 du mois N+1 par exemple, est qu'on récupère le maximum de déclarations du mois N *en retard* par rapport au pic de chargement du 20 du mois N+1.

Ainsi, dans la requête SQL ci-dessus, pour récupérer les données du mois MM de l'année AAAA, la clause `WHERE` est : 

```SQL
  WHERE [dbo].[NomTable].[IdDateChargement] >= AAAA(MM+1)09
  AND [dbo].[NomTable].[IdDateChargement] < AAAA(MM+2)09
```

Si besoin, les clauses temporelles peuvent être récupérées dans le tableur `core/python/raw_files_management/raw_DSN_selection_scripts/WHERE_time_clauses.xlsx`.

L'import n'étant pas automatique, **le 9 de chaque mois, il faut envoyer un message à VDDAL pour qu'il lance l'import de données**. Une fois cela réalisé, il faut lancer l'intégration mensuelle.

## Accès aux données source

Pour plus d'informations sur l'hébergement des données dans l'infrastructure Champollion, se référer TO DO : mettre lien.

Les chemins d'accès aux données sont déterminés :

1. d'une part à l'aide des variables d'environnement suivantes :

    - `WORKFLOW_ARCHIVES_DATA_PATH` : dossier hébergeant les archives compressées de fichiers DSN ;
    - `WORKFLOW_RAW_DATA_PATH` : dossier hébergeant les fichiers DSN après décompression ;
    - `WORKFLOW_TEST_DATA_PATH` : dossier hébergeant les fichiers DSN de test ;
    - `WORKFLOW_SOURCES_DATA_PATH` : dossier hébergeant les auter fichiers de données non DSN.

2. d'autre part via les paramètres passés aux [orchestrateurs] (TO DO : mettre lien) sous [format jinja](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html):

    - `{{ params.filepath }}`
    - `{{ params.filetype }}`
    - `{{ params.foldername }}`
    - `{{ params.filedate }}`
    - `{{ params.filename }}`

afin d'accèder à un fichier `{{ params.filepath }}/{{ params.filetype }}/{{ params.foldername }}_{{ params.filedate }}/{{ params.filename }}_{{ params.filedate }}.csv` ou `{{ params.filepath }}/{{ params.filename }}.csv` par exemple.