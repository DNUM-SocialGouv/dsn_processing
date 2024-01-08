# DSN PROCESSING

Le projet [Champollion](https://www.eig.numerique.gouv.fr/defis/champollion/), débuté en septembre 2022, avait pour objectif d'exploiter les données des [Déclarations Sociales Nominatives](https://entreprendre.service-public.fr/vosdroits/F34059) (DSN). Une partie du projet a donc été consacré à la construction d'une base, nommée Champollion.

Sourcée sur les données des [déclarations sociales nominatives](https://www.net-entreprises.fr/media/documentation/dsn-cahier-technique-2023.1.pdf) (DSN). Elle a été élaborée pour être facilement exploitée à des fins opérationnelles par les directions métier. Pour ce faire, les données DSN sont, chaque mois, re-structurées, chaînées dans le temps et consolidées.

Le périmètre de la base Champollion est voué à s'étendre avec l'ajout de nouveaux cas d'usage. A date, elle n'est utilisée que pour l'application [VisuDSN](https://github.com/DNUM-SocialGouv/champollion-front) à destination de l'Inspection du Travail.

## Installation

Dans la suite on considère la variable `DSN_PROCESSING_REPOSITORY_PATH` qui est le chemin vers le dossier `dsn_processing`. Pour exporter cette variable d'environnement, on pourra utiliser la commande `export DSN_PROCESSING_REPOSITORY_PATH=...`.

### Pré-requis

Pour pouvoir exécuter les commandes suivantes, il faut que les paquets suivants soient installés sur votre machine :

- `python39`
- `libpq-dev`
- `python39-devel`
- `p7zip*`

Si votre gestionnaire de paquets est `dnf`, vous pouvez vérifier l'installation de ces derniers avec les commandes suivantes :

``` bash
dnf list installed | grep 'python39'
dnf list installed | grep 'libpq-dev'
dnf list installed | grep 'python39-devel'
dnf list installed | grep 'p7zip*'
```

Il faut également vérifier que le raccourci syntaxique de `p7zip` est le bon (`7za`) avec la commande `7za -version`.

### Créer un lien symbolique pour importer `dsn_processing` comme une librairie python

```bash
export PYTHON_LIB_PATH=...
ln -s ${DSN_PROCESSING_REPOSITORY_PATH} ${PYTHON_PATH}/site-packages/dsn_processing
```

Le `PYTHON_LIB_PATH` est le chemin qui pointe sur votre dossier `lib` Python. Si j'utilise un environnement virtuel nommé `lab` stocké dans un dossier `env` à la racine de ma session par exemple, j'aurais `PYTHON_LIB_PATH=~/env/lab/lib/python3.9`.

### Installer les `requirements.txt`

Pensez à [exporter](#exporter-les-variables-denvironnement) les variables `HTTP_PROXY` et `HTTPS_PROXY` avant de réaliser cette étape. Les valeurs de ces variables sont disponibles [ici](https://msociauxfr.sharepoint.com/:t:/r/teams/EIG71/Documents%20partages/General/Commun/D%C3%A9veloppement/.env.prefilled/.env.dsn_processing.prefilled.txt?csf=1&web=1&e=GchPW6).

```bash
pip install -r ${DSN_PROCESSING_REPOSITORY_PATH}/dsn_processing/requirements.txt
```

### Installer `pre-commit`

Pensez à [exporter](#exporter-les-variables-denvironnement) les variables `HTTP_PROXY` et `HTTPS_PROXY` avant de réaliser cette étape. Les valeurs de ces variables sont disponibles [ici](https://msociauxfr.sharepoint.com/:t:/r/teams/EIG71/Documents%20partages/General/Commun/D%C3%A9veloppement/.env.prefilled/.env.dsn_processing.prefilled.txt?csf=1&web=1&e=GchPW6).

```bash
cd ${DSN_PROCESSING_REPOSITORY_PATH}/dsn_processing
pre-commit install
```

### Exporter les variables d'environnement

Les variables d'environnement à définir sont répertoriées dans le fichier [dsn_processing/.env.example](https://gitlab.intranet.social.gouv.fr/champollion/dsn_processing/blob/dev/.env.example). Il faut donc créer un fichier `.env.dsn_processing` stocké à la racine de la session par exemple. Un fichier pré-rempli est disponible [ici](https://msociauxfr.sharepoint.com/:t:/r/teams/EIG71/Documents%20partages/General/Commun/D%C3%A9veloppement/.env.prefilled/.env.dsn_processing.prefilled.txt?csf=1&web=1&e=E74wja).

Pour exporter les variables de ce fichier, on pourra utiliser la fonction `bash` suivante.

```bash
export $(grep -v '^#' ~/.env.dsn_processing | xargs)
```

On peut ajouter cette ligne à son fichier `~/.bashrc` si l'on souhaite que l'export de ces variables d'environnement soit automatique à l'ouverture d'un terminal.

A noter que des variables d'environnement complémentaires sont nécessaires pour déployer la brique [Airflow](docs/integration/pipeline/dags_et_orchestrateurs.md#déploiement).

## Contributions

### Auteurs (décembre 2023)

- Auteur principal : [Margot COSSON](https://github.com/margotcosson)
- Co-auteurs : [Léo GUILLAUME](mailto:leoguillaume1@gmail.com); [Yan ZHI](mailto:yan.zhi@sg.social.gouv.fr)

### Contribuer au projet

Ce code est principalement hébergé sur un gitlab interne. Sa [version publique sur Github](https://github.com/DNUM-SocialGouv/dsn_processing) est un mirroir du repository gitlab.

#### Tests unitaires

Le fichier `.pre-commit-config.yaml` assure l'exécution de tests unitaires au moment d'un commit. Les tests dont le résultat n'empêche pas le commit sont étiquettés avec la balise `non-blocking`. Pour s'exécuter, les tests ont besoin que les variables d'environnement listées dans le fichier [`dsn_processing/.env.example`](https://gitlab.intranet.social.gouv.fr/champollion/dsn_processing/blob/dev/.env.example) soient [définies](#exporter-les-variables-denvironnement).

#### Nomenclature des commits

La nomenclature retenue pour les commits est la suivante : 

```
<type de tâche>(<objet de la tâche>) : <numéro de la tâche JIRA> <résumé>
```

Le type de la tâche pouvant être `feat` pour une nouvelle fonctionnalité, `chore` pour une tâche de maintenance, `fix` pour la correction d'un bug et `doc` pour de la documentation.

Exemple :

```
chore(all) : CHAM-506 move data processing code to an independent repo
```

#### Pousser sur github

1. Configurez votre accès à github en générant un [personal access token](https://github.com/settings/tokens) et en exécutant la commande suivante : 
    ```bash
    git config --global credential.github.com.token PERSONAL_ACCESS_TOKEN
    ```
    Dans la suite, si une fenêtre github s'ouvre, n'ouvrez pas la redirection vers github.com, restez sur l'interface VsCode, entrez votre nom d'utilisateur puis saisissez le token d'accès comme mot de passe.

2. Dans le dossier `dsn_processing`, ajoutez la remote github :
    ```bash
    git remote add github-upstream https://github.com/DNUM-SocialGouv/dsn_processing.git
    ```

3. Poussez la dernière version du code :
    ```bash
    git fetch upstream dev
    git checkout -B dev -t upstream/dev
    git push github-upstream dev
    ```
