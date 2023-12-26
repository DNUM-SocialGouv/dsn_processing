# Exécution des scripts d'intégration : dags et orchestrateurs

Un DAG (directed acyclic graph) est une liste ordonnée de tâches matérialisées par des scripts Python ou SQL. En d'autres termes, il s'agit d'une liste de scripts qui sont exécutés successivement selon l'ordre de la liste.

On parle d'orchestrateur pour parler d'une brique qui permet l'exécution de scripts seuls ou de DAG. A noter qu'un orchestrateur peut aussi comporter des fonctionnalités de planification afin de programmer l'exécution automatique de tâches.

## Orchestrateurs de scripts

Deux orchestrateurs aux usages complémentaires sont disponibles :

- un orchestrateur Bash (avec connexion à la base via Python) pour les besoins du développement ;
- un orchestrateur [Airflow](https://airflow.apache.org/docs/) pour la production.

Afin de limiter les risques de divergence entre les deux orchestrateurs, la majorité des dags définis dans Airflow le sont sur la base de ceux définis dans l'orchestrateur Bash.

### Orchestrateur Bash

Le code de l'orchestrateur Bash se situe dans le dossier `pipeline/bash/`. Cet orchestrateur ne doit être utilisé qu'en développement. Bien qu'utile de par sa flexibilité et sa prise en main rapide, il n'offre pas les standards de robustesse, de tracabilité et de sécurité attendus pour le lancement d'intégration en production. 

#### Le coeur : `orchestrator.py`

Il s'appuie sur un fichier Python qui permet l'exécution d'un script SQL tel que : 

```bash
usage: orchestrator.py [-h] -s SCRIPT [-cmf COPY_MONTHLY_FILE] [-csf COPY_STATIC_FILE] [-d DATE] [-f FOLDER_TYPE]

Bash orchestrator

optional arguments:
  -h, --help            show this help message and exit
  -s SCRIPT, --script SCRIPT
                        Name of the SQL script to execute (<dag_name/script_name.sql>).
  -cmf COPY_MONTHLY_FILE, --copy_monthly_file COPY_MONTHLY_FILE
                        If importing SQL script, the name of the monthly csv file (<csv_name>).
  -csf COPY_STATIC_FILE, --copy_static_file COPY_STATIC_FILE
                        If importing SQL script, the name of the static csv file (<csv_name>).
  -d DATE, --date DATE  Declaration date (format : YYYY-MM-DD).
  -f FOLDER_TYPE, --folder_type FOLDER_TYPE
                        Folder type (raw or test).
```

A noter qu'il ne prend pas en argument les paramètres de connexion à la base de données (serveur, port, identifiant, etc.). En effet, les valeurs utilisées sont celles des variables d'environnement (`POSTGRES_DB, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST`) d'où l'importance de les définir correctement avant de lancer l'orchestrateur. Sa verbosité peut également être modulée à l'aide de la variable d'environnement `BASH_ORCHESTRATOR_VERBOSE` (True/False).

#### Les dags Bash

Néanmoins, sauf exception, il sera rare de l'utiliser tel quel. En effet, si on souhaite exécuter un seul script, on préfèrera copier-coller directement la requête dans PGAdmin. 

L'intérêt du fichier Python est qu'il permet d'exécuter séquentiellement plusieurs scripts, autrement dit, d'exécuter un dag en une ligne de commande : 

```bash
bash pipeline/bash/dags/init_database.sh
bash pipeline/bash/dags/monthly_integration.sh <year> <month> <folder_type>
bash pipeline/bash/dags/update_static_files.sh <year>
bash pipeline/bash/dags/historical_integration.sh <start_year> <end_date> <folder_type>
bash pipeline/bash/dags/test_integration.sh
bash pipeline/bash/dags/mock_integration.sh
bash pipeline/bash/dags/anonymous_integration.sh
```

### Orchestrateur Airflow

Le code de l'orchestrateur Airflow se situe dans le dossier `pipeline/airflow/`. Si cet orchestrateur offre une interface de suivi et des briques d'orchestration robustes, il manque de flexibilité. En phase de test, on préfèrera l'orchestrateur Bash.

#### Déploiement

Chaque Airflow est déployé pour un serveur de base donné, spécifié à l'aide des variables d'environnement `POSTGRES_HOST` et `POSTGRES_PORT`.

##### Pré-requis 

Pour que les étapes suivantes fonctionnent, il faut qu'une connexion au Hub de stockage des images ait été activée (TO DO : mettre lien).

##### En dynamique ou en statique

Si Airflow est déployé sur une machine qui héberge le code, on peut *déployer* l'ochestrateur de manière dynamique. Les dags sont alors définis directement par le dossier contenant le code. Pour ce faire, on décommentera dans le fichier [docker-compose](./../../../pipeline/airflow/docker-compose.yaml) les deux lignes suivantes : 

```yaml
    - ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/airflow/dags:/opt/airflow/dags                  # only for development
    - ${DSN_PROCESSING_REPOSITORY_PATH}:/home/airflow/code/dsn_processing                        # only for development
```

Dans le cas inverse, Airflow est déployé en statique, les dags sont définis par le code contenu dans l'image. Contrairement au déployement dynamique, la modification du code nécessite donc de repasser par une étape de build.

##### Etape de build

Trois images sont nécessaires pour la mise en route de ce service :

-  `airflow/common`
-  `airflow/postgres`
-  `airflow/redis` 

1. Créez un fichier de variables d'environnement sur la base du [fichier d'exemple](../../../.env.example). Veillez à incrémenter la valeur des tags des trois images (variables `AIRFLOW_COMMON_IMAGE_TAG`, `AIRFLOW_POSTGRES_IMAGE_TAG` et `AIRFLOW_REDIS_IMAGE_TAG`) afin de ne pas écraser les versions antérieurs (sauf en cas de correctif sur une précédente image).

2. Vérifiez que vous êtes sur la version du code que vous souhaitez déployer.

3. Compilez et poussez les images (avec `ENV_FILE_PATH`, le chemin vers votre fichier de variables d'environnement)

    ```bash
    bash pipeline/airflow/build.sh -e ENV_FILE_PATH -p
    ```

4. Connectez-vous au Hub pour vérifier que les images ont bien été poussées.

##### Etape de run 

1. Connectez-vous sur la VM devant héberger le service avec le compte souhaité pour le déploiement des conteneurs.

2. Récupérez le fichier [docker-compose](../../../pipeline/airflow/docker-compose.yaml) et le fichier d'environnement tel que défini à l'étape 1 du build. Copiez les sur la machine.

3. Mettez en service les conteneurs avec la commande suivante (remplacez `<path>` par le chemin d'accès à votre fichier de variables d'environnement) :

    ```bash
    ENV_FILE_PATH=<path>/.env && \
    docker compose --env-file $ENV_FILE_PATH stop && \
    docker compose --env-file $ENV_FILE_PATH rm -f && \
    docker compose --env-file $ENV_FILE_PATH up --detach --pull always
    ```

4. Vérifiez que le statut des containers est "up" avec la commande `docker ps`.

##### Dans l'environnement Champollion

Le Hub de stockage des images est le [Nexus](https://10.252.1.10/#browse/browse:Champollion), on effectue l'étape de build sur la VM LAB et on déploie l'Airflow sur la VM WORKFLOW avec le compte `svc.champollion`. A noter qu'on peut aussi déployer l'Airflow sur la VM LAB en test et on pourra alors l'utiliser de manière [dynamique](#en-dynamique-ou-en-statique).

#### Accès à l'interface d'Airflow

Pour accéder à l'interface d'Airflow, il suffit de se connecter sur la VM où le service est déployé puis de forward le port sur lequel il est exposé (variable `AIRFLOW_PORT`, par défaut 8080).

Le login et le mot de passe sont définis par les valeurs des variables d'environnement `AIRFLOW_WWW_USER_USERNAME` et `AIRFLOW_WWW_USER_PASSWORD` lors du déploiement.

#### Les dags Airflow

Les dags Airflow sont définis par les fichiers python du dossier [dags](TO DO : mettre lien). La plupart des ces fichiers font appel à la fonction `register_tasks` du fichier [utils.py](TO DO : mettre lien) qui écrit les dags automatiquement à partir des scripts listés dans les [dags Bash](#les-dags-bash).

Pour en savoir plus sur l'utilisation d'Airflow, se rendre sur la [documentation](https://airflow.apache.org/docs/) de l'outil. En substance : 

- La page d'accueil est la liste des dags.
- Les dags peuvent être lancés à l'aide du triangle bleu (*Trigger DAG*) de la colonne Actions. Si le dag requiert un paramétrage, un menu intermédiaire permet de fixer la valeur des paramètres. En particulier, cela permet de choisir la base de données (au sein du serveur indiqué lors du déploiement).

A noter qu'il n'existe pas de dag `historical_integration`, cette procédure est orchestrée par un script bash qui lance des dags `monthly_integration` successifs dans Airflow. Pour plus d'information, voir la section [Procédure de reprise historique](#procédure-de-reprise-historique).

#### Automatisation de l'intégration mensuelle

En l'absence d'automatisation de l'import des données source, l'automatisation des intégrations dans Airflow a été désactivée. Pour configurer une orchestration automatique, il faut paraméter le DAG dans le fichier python avec le paramètre [`schedule_interval`](https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html) tel que :

```py
with DAG(
    ...
    schedule_interval=f"0 8 10 * *",  # si on souhaite intégrer le 10 de chaque mois à 8h par exemple
    ...   
)    
```

## Dags

La liste des DAGs disponibles est la suivante :

|  | Description | Périodicité | Bash | Airflow |
|---|---|---|---|---|
| check_database_connection | Test de la connexion à la base de données. | NA |  | x |
| init_database | Initialisation de la base de données. | NA | x | x |
| monthly_integration | Intégration mensuelle de données. | Le 10 de chaque mois après [réception des données source](import_et_acces_donnees_source.md#planning-des-imports). | x | x |
| update_static_files | Mise à jour des fichiers de contexte, dits fichiers statiques. | Une fois par an. | x | x |
| update_database | Mise à jour des tables contextuelles, dites statiques. | Une fois par an. | x | x |
| historical_integration | Intégration successive de plusieurs mois de données. | NA | x | x (via un fichier bash) |
| test_integration | Intégration de test. | NA | x |  |
| mock_integration | Intégration des données mockées. | NA | x | x |
| anonymous_integration | Création d'un schéma de données anonymisées. | NA | x |  |

Dans la suite de cette section, on décrit les spécificités de chaque DAG.

TO DO : mettre lien sur chaque x du tableau

### `check_database_connection`

Les logs du DAG permettent de consulter les informations de connexion afin de vérifier qu'il s'agit de la bonne base de données.

### `init_database`

Le DAG `init_database` fait appel au DAG `update_database` afin de compléter dès l'initilisation de la base les tables statiques.

### `monthly_integration`

Le DAG commence par une étape d'extraction des données source. Pour ce faire, on fait appel au script [`extract_archive.sh`] (TO DO : mettre lien).  A la fin de l'exécution, on supprime les données désarchivées grâce au script [`remove_extracted.sh`] (TO DO : mettre lien). A noter que cette suppression est systématique via l'orchestrateur Bash que le DAG ait fini en erreur ou non, ce n'est pas le cas dans Airflow.

#### Spécificités dans Airflow

Le DAG dans Airflow commence par une tâche `get_start_task` qui vérifie que le statut de la base (champ `sys.current_status.status`) est en statut `SUCCESS`, ce qui indique que la précédente intégration s'est finie correctement. Dans le cas inverse (statut `ONGOING`), le DAG est interrompu en erreur. Si le résultat est positif, il change le statut de la base à `ONGOING` pour signifier le fait que l'intégration commence.

Le DAG exécute ensuite une salve de vérifications sur les données d'entrée :

- est-ce que le dossier de données source existe ?
- est-ce que les fichiers de données source existent ?
- est-ce que les fichiers ont une taille cohérente ?
- est-ce que les fichiers comportent le bon délimiteur de colonnes ?
- est-ce que les fichiers ont les bonnes colonnes dans le bon ordre ?

Les fonctions implémentées pour ces vérifications sont celles du fichier `check_conformity_raw_files.py` (TO DO mettre lien) qui s'appuie sur le fichier de configuration `raw_files_config.json` (TO DO : mettre lien).

Si le paramètre `do_backup` a été fixé à `True`, un backup de la base est réalisé à l'issue de l'intégration des données. Le script utilisé à cette fin est `database_backup.sh` (TO DO : mettre lien). Pour plus d'informations, TO DO : mettre lien

Si toutes les étapes du DAG ont été un succès, la tâche finale `get_successful_end_task` passe le statut de la base d'`ONGOING` à `SUCCESS`.

#### Performances sur les données réelles

La durée d'exécution du DAG `monthly_integration` augmente avec la taille de la base. Si pour le premier mois intégré, elle est d'1h30 environ, elle monte progressivement jusqu'à 8h pour le 60e mois intégré en suivant une évolution linéraire de coefficient 6-7. Ce temps d'intégration devrait se stabiliser lorsque la suppression des données à l'aide du script `remove_old_data.sql` (TO DO : mettre lien) débutera au bout de 72 mois d'historique.

### `update_static_files`

/!\ Ce DAG nécessite une connexion internet. A date, ce DAG ne fonctionne pas donc pas à cause d'un bug du proxy. TO DO : mettre tâche JIRA propre.

Le DAG fait appel aux fonctions Python des fichiers `generate_static_table_files.py` et `generate_holiday_calendar.py` du dossier `raw_files_management` (TO DO : mettre liens). Ces fonctions accèdent aux données source sur internet et la bonne exécution du DAG requiert donc une connexion internet.

A noter que pour ce DAG, la version Airflow ne se base pas sur celle Bash pour des raisons de simplicité d'implémentation de la fonction `register_tasks`.

### `update_database`

Aucune spécificité.

TO DO : ajouter dag airflow

### `historical_integration`

TO DO : mettre lien vers grande partie

### `test_integration`

Le DAG `test_integration` encode plusieurs étapes :

* la création d'échantillons de fichiers bruts (facultatif) ;
* la création des fichiers de comparaison ;
* l'intégration de ces échantillons via les scripts d'intégration (i.e *ce qui obtenu* par l'intégration);
* l'intégration des tables de référence qui correspondent à *ce qui est attendu* comme données de sortie ;
* la comparaison de *ce qui est obtenu* avec *ce qui est attendu*.

Pour plus d'informations sur la base de test et la nature des données *attendues* vs *obtenues*, se référer à TO DO : mettre lien.

#### Fichier de référence pour la construction de la base de test

Le fichier à l'origine de la construction de la base de test est `source_file_test_data.xlsx` (TO DO : mettre lien):

- l'onglet *Sujets* détaille les différents cas de figure pour lesquelles les scripts d'intégration doivent être testés ;
- l'onglet *Input* liste les `IdContrat` et `DateChargement` des lignes qui doivent être présentes dans l'échantillon du fichier brut des contrats ;
- les onglets *Output ...* répertorient les données attendues dans les différentes tables en sortie des scripts d'intégration.

Ces onglets sont remplis à la main par l'équipe de développement. A noter que la moindre modification de l'onglet *Input* nécessite donc une répercusion à la main sur les onglets *Output*. Les données des onglets *Output* sont des données réelles, il convient néanmoins de ne pas y inclure de données personnelles. On cherche seulement à tester les mécaniques des scripts d'intégration.

#### Création des échantillons de fichiers bruts

A partir du fichier de référence `source_file_test_data.xlsx`, on génère les échantillons de fichiers bruts DSN. Cette opération est effectuée par la fonction `generate_input_data_files` du fichier `generate_test_data_files.py` (TO DO : mettre lien). L'opération est assez lourde puisqu'elle nécessite de :

- décompresser les archives de données brutes ;
- et sélectionner toutes les données liées directement ou indirectement aux `IdContrat` renseignés dans l'onglet *Input*.

Cette étape n'est nécessaire qu'en cas de modification de l'onglet *Input* du fichier de référence. Elle est exécutée lors de l'appel du fichier `generate_test_data_files.py` si une balise `-i` a été ajoutée.

Les fichiers ainsi générés sont stockés dans le dossier correspondant à la variable d'environnement `WORKFLOW_TEST_DATA_PATH`.

#### Création des fichiers de comparaison

A partir du document de référence `source_file_test_data.xlsx`, on génère également les fichiers `csv` correspondants aux tables *expected*. Cette étape est implémentée par la fonction `generate_expected_data_files` du fichier `generate_test_data_files.py`.

Les fichiers ainsi générés sont stockés dans le dossier correspondant à la variable d'environnement `WORKFLOW_SOURCES_DATA_PATH`.

#### Intégration des données de test via les scripts d'intégration

Au lancement du DAG, la variable `POSTGRES_DB` est automatiquement basculée sur la valeur `test` pour intégrer les données en base de test. Le DAG `init_database` est appelé pour initialiser la base de test. Par la suite, le DAG `historical_integration` est exécuté avec les paramètres suivants renseignés en dur dans le fichier `test_integration.sh` :

- `start_date` = 2022-01-01 → correspond au à la date la plus ancienne des données de test (la commande `ls $WORKFLOW_TEST_DATA_PATH` peut être utilisée pour déterminer cette borne inférieure) ;
- `end_date` = 2022-09-01 → correspond au à la date la plus récente des données de test (la commande `ls $WORKFLOW_TEST_DATA_PATH` peut être utilisée pour déterminer cette borne supérieure) ;
- `folder_type`= `test` → permet d'aller chercher les données de test, c'est-à-dire les données du dossier `WORKFLOW_TEST_DATA_PATH`.

#### Intégration des données *attendues*

Les scripts du dossier *test_integration* (TO DO : mettre lien) sont ensuite appelés pour intégrer les tables de référence que contiendra le schéma `test`.

#### Vérification de la mise en qualité des données (tests unitaires)

Le fichier `tests.py` (TO DO : mettre lien) implémente les fonctions de comparaison entre les données *obtenues* via les scripts d'intégration et les données *attendues* des tables homonymes.

La comparaison comprend plusieurs axes :
* les ensembles de tuples identifiants des tables *obtenues* et *attendues* sont identiques ;
* pour chaque tuple identifiant, les données renseignées dans la table *attendue* (non nulles) sont identiques à celles de la table *obtenue*.

Les comparaisons effectuées sur les champs `date_fin_effective` et `statut_fin` de la table `contrats` font figures d'exception. La table `expected_contrats_comparisons` sert à connaître le type de comparaison à effectuer pour ces deux champs. Si `expected_contrats_comparison.date_fin_effective_comparison` est égal à:
* 1 alors la date de fin effective *obtenue* doit être strictement égale à celle *attendue* (idem pour le statut);
* 2, elle doit être supérieure ou égal à celle *attendue* (idem pour le statut);
* ni 1 ni 2, on ne peut pas faire de comparaison (idem pour le statut).

Au sein du DAG, les tests sont lancés à l'aide de la commande suivante :

`pytest ${DSN_PROCESSING_REPOSITORY_PATH}/tests/tests.py`

On pourra également les lancer avec cette même commande en dehors du DAG `test_integration` si on le souhaite.

### `mock_integration`

Le fichier à l'origine de la construction de la base mockée est `source_file_mock_data.xlsx` (TO DO : mettre lien). Il comprend un onglet par table dynamique de la base. Le DAG `mock_integration` vient donc :

- exporter la variable d'environnement `POSTGRES_DB` à la valeur `mock` ;
- convertir les données de cet excel en fichiers `csv` stockés dans `WORKFLOW_SOURCES_DATA_PATH` à l'aide de la fonction `generate_mock_data_files` du fichier `generate_mock_table_files.py` (TO DO : mettre lien) ;
- initialiser la base de données `mock` à l'aide du DAG `init_database` ;
- intégrer les fausses données à l'aide des scripts du dossier `mock_integration` (TO DO : mettre lien).

### `anonymous_integration`

Sur l'espace Teams, un [excel](https://msociauxfr.sharepoint.com/:x:/r/teams/EIG71/Documents%20partages/General/Commun/D%C3%A9veloppement/P%C3%A9rim%C3%A8tre%20de%20la%20base%20anonymis%C3%A9e%20pour%20les%20devs.xlsx?d=w652861c744a74cd3b11f0cf5431847a4&csf=1&web=1&e=fPbJn8) permet de répertorier les SIRET présents dans le schéma anonymisé. La procédure pour étendre le périmètre de données est la suivante :

1. Le développeur, qui souhaite ajouter le SIRET x au schéma anonymisé, l'ajoute à l'excel avec un commentaire indiquant la raison de sa demande.
2. Un membre de l'équipe ayant accès au schéma public ouvre une merge request qui permet l'ajout de ce SIRET dans le fichier `anonymous_database_selection.csv` (TO DO : mettre lien) délimitant le périmètre des données anonymisées. Il relance ensuite la création du schéma `anonymous` sur la base de son choix, à l'aide du dag `anonymous_integration.sh`.
3. Il faut ensuite mettre à jour l'excel de l'espace Teams avec les données issues de la requête SQL suivante : 

    ```sql
    SELECT 
        TO_CHAR(R.etablissement_key, 'fm00000000000000') AS "vrais_siret", 
        TO_CHAR(F.etablissement_key, 'fm00000000000000') AS "siret_anonymises",
        CASE WHEN R.code_naf = '7820Z' THEN 'ETT' ELSE 'ETU' END AS "etu_ett",
        CASE WHEN S.etablissement_key IS NOT NULL THEN 'Oui' ELSE 'Non' END AS "ajout_direct",
        CASE WHEN S.comment IS NOT NULL AND S.comment != 'ETT référencé' THEN S.comment ELSE '' END AS "commentaire"

    FROM anonymous.etablissements AS F
    INNER JOIN public.etablissements AS R 
        ON F.etablissement_id = R.etablissement_id
    LEFT JOIN anonymous.selection_etablissements as S
        ON S.etablissement_key = R.etablissement_key
    ORDER BY R.etablissement_key
    ```
4. Le développeur peut alors retourner sur l'excel afin de connaître le SIRET *anonyme* de l'établissement qu'il cherchait à ajouter au schéma anonymisé.

## Procédure de reprise historique 

Lorsqu'on souhaite intégrer plusieurs mois d'affilée on parle de procédure de reprise historique. **Avant de lancer une procédure de reprise historique avec les données réelles, veuillez consulter la section [Lancer une procédure de reprise historique avec les données réelles via Airflow](#lancer-une-procédure-de-reprise-historique-avec-les-données-réelles-via-airflow)**.

### DAGs `historical_integration`

*Le DAG `historical_integration` fait figure d'exception côté Airflow, il s'agit d'un fichier Bash qui lance l'exécution successive de DAG `monthly_integration` Airflow. Pour comprendre comment l'utiliser correctement pour intégrer des données, la section [Lancer une procédure de reprise historique avec les données réelles via Airflow](#lancer-une-procédure-de-reprise-historique-avec-les-données-réelles-via-airflow) doit être consultée.*

Les paramètres du DAG `historical_integration` sont les suivants :

| Paramètre | Bash | Airflow |
|---|---|---|
| Premier mois à intégrer | Premier paramètre `start_date` | Paramètre `start` à faire précéder de la balise `--start` |
| Dernier mois à intégrer | Deuxième paramètre `end_date` | Paramètre `end` à faire précéder de la balise `--end` |
| Base de donnée | Déterminé par la variable d'environnement `POSTGRES_DB` | Paramètre `database` à faire précéder de la balise `--database` (valeur par défaut : `champollion`) |
| Type de fichiers | Troisième paramètre `folder_type` | Paramètre `filetype` à faire précéder de la balise `--filetype` (valeur par défaut : `raw`) |
| Backup | Non implémenté (orchestrateur de développement) | Balise `--do_backup` à ajouter |
| Verbosité / logs | Verbosité déterminée par la variable d'environnement `BASH_ORCHESTRATOR_VERBOSE` | Paramètre `log` à faire précéder de la balise `--log` |

Tous les mois de données entre le premier et le dernier mois indiqués sont intégrés successivement à l'aide du DAG `monthly_integration`.

Le DAG Bash s'utilise donc de la manière suivante :

```bash
bash pipeline/bash/dags/historical_integration.sh <start_year> <end_date> <folder_type>
# example
bash pipeline/bash/dags/historical_integration.sh 2019-01-01 2023-01-01 test
```

Quant à celui Airflow, il est exécutable tel que :

```bash
Syntax: bash historical_integration.sh [--help|--start|--end|--database|--filetype|--log]
options:
--help          display this help and exit
--start         first month to integrate (required)
--end           last month to integrate (included) (required)
--database      optional, data base connexion: 'champollion' (default), 'test' or 'mock'
--filetype      optional, data path of DSN files: 'raw' (default) or 'test'
--do_backup     optional, if added, backups are performed
--log           optional, path directory to export log file

# example
bash pipeline/airflow/dags/historical_integration.sh --start 2019-01-01 --end 2023-01-01 --database test --filetype test
```

Si la variable d'environnement `COMPOSE_PROJECT_NAME` n'a pas été exportée, il est nécessaire de la ré-exporter avant de lancer le DAG Airflow via la commande `export COMPOSE_PROJECT_NAME=...` avec la valeur renseignée dans le fichier d'environnement utilisé lors du déploiement d'Airflow.

### Interactions `pipeline/airflow/dags/historical_integration.sh` - Airflow

Le script `pipeline/airflow/dags/historical_integration.sh` lance l'appel à tous les DAGs Airflow `monthly_integration` pour les différents mois à intégrer. Il n'attend donc pas l'exécution du DAG `monthly_integration` du mois M-1 pour lancer celui du mois M. Néanmoins, étant donné que le DAG Airflow `monthly_integration` a un paramètre `max_active_runs=1`, les DAGs s'exécute successivement. Dès lors, lorsque le script `historical_integration.sh` a fini de s'exécuter (en quelques dizaines de secondes), tous les DAGs `monthly_integration` à exécuter sont recensés dans l'Airflow, le premier est en exécution (cercle vert) et les autres en attente (cercles gris).

Si jamais un DAG tombe en erreur, les suivants sont interrompus dès leur première tâche `get_start_task` étant donné qu'il a laissé la base en statut `ONGOING`. La procédure de reprise d'erreur se fait alors à l'aide d'un backup complet de la base pour la restorer dans son état précédant le début de ce DAG ayant échoué. Pour plus d'informations, voir la section [Lancer une procédure de reprise historique avec les données réelles via Airflow](#lancer-une-procédure-de-reprise-historique-avec-les-données-réelles-via-airflow).

### Lancer une procédure de reprise historique avec les données réelles via Airflow

On liste la démarche à suivre ci-dessous. A noter que ces procédures n'ont pas été automatisées car la reprise historique avec données réelles est un processus coûteux en temps de calcul qui ne doit être exécuté qu'à la suite à de changements majeurs dans les scripts.

1. Déployer Airflow sur la VM WORKFLOW avec la bonne version du code et les variables d'environnement correspondant au serveur de base choisi (pour plus d'informations, voir [la section relative au déploiement d'Airflow](#déploiement)).

2. Initialiser la base de données à l'aide du DAG `init_database`.

3. S'assurer que le champ `sys.current_status.status` a la valeur `SUCCESS`.

4. Désactiver les logs transactionnels (sinon le serveur hébergeant les logs va saturer) :

    ```sql
    ALTER SYSTEM SET archive_command TO '/bin/true'; 
    SELECT pg_reload_conf();
    SHOW archive_command; -- doit être égal à /bin/true
    ```

5. Récupérer le fichier `pipeline/airflow/dags/historical_integration.sh` dans le code (TO DO : mettre lien) et le copier sur la VM WORKFLOW.

6. Exporter la variable d'environnement `COMPOSE_PROJECT_NAME` à l'aide de la commande `export COMPOSE_PROJECT_NAME=...` avec la valeur renseignée dans le fichier d'environnement utilisé lors du déploiement d'Airflow.

7. Lancer le DAG `historical_integration` à l'aide du fichier `pipeline/airflow/dags/historical_integration.sh` (pour plus d'informations, voir la section [DAGs `historical_integration`](#dags-historical_integration)).

8. **En cas d'interruption** :
    1. Via l'interface Airflow, trouver le premier DAG `monthly_integration` (c.a.d mois) qui a fini en erreur (les suivants finissent en erreur dès la première tâche `get_start_task`, voir [Interactions `pipeline/airflow/dags/historical_integration.sh` - Airflow](#interactions-pipelineairflowdagshistorical_integrationsh---airflow)).
    2. Corriger le bug dans les scripts et re-déployer l'Airflow avec la nouvelle version du code.
    3. Remettre la base dans son état précédant ce DAG ayant fini en erreur. Pour ce faire, utiliser le backup réalisé à l'aide de la tâche `database_backup` lors du dernier DAG fructueux. Pour connaître les commandes à exécuter, voir TO DO : mettre lien champolib.
    4. Repasser le statut de la base à `ONGOING` grâce à la commande : `UPDATE sys.current_status SET status = 'SUCCESS'`.
    5. Reprendre à l'étape 3 avec comme premier mois à intégrer le mois du DAG ayant échoué.
    
9. Une fois la reprise historique terminée, réactiver les logs transactionnels :

    ```sql
    ALTER SYSTEM SET archive_command TO '/logiciel/pgsql-15/archive_xlog.sh "<IP Data de la DB>" %p';
    SELECT pg_reload_conf();
    SHOW archive_command; -- doit être égal à /logiciel/pgsql-15/archive_xlog.sh "<IP Data de la DB>" %p
    ```

