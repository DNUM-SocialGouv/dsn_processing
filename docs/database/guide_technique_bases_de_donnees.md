# Guide technique des bases de données

Qu'il s'agisse de serveurs de base conteneurisés pour les besoins du développement ou de serveur de base de production, on instancie trois bases de données pour les usages suivants :

- `champollion` : la base principale, qui contient les données réelles et complètes ;
- `mock` : une base de fausses données ;
- `test` : une base de test qui contient des données réelles de test.

Pour plus d'informations sur l'infrastructure retenue pour les bases, se référer à TO DO : mettre lien.

## Initialisation

### Création des bases 

*A noter que pour les serveurs de base conteneurisés, la création de ces bases est automatique lors du déploiment du serveur.*

Pour créer ces trois bases, on se connecte à la base `postgres` (il est nécessaire de ne pas être connecté via une version pré-existante de l'une des trois bases) et on exécute la commande suivante : 

```sql
DROP DATABASE IF EXISTS champollion WITH (FORCE);
DROP DATABASE IF EXISTS mock WITH (FORCE);
DROP DATABASE IF EXISTS test WITH (FORCE);

CREATE DATABASE champollion
    WITH
    OWNER = champollion
    ENCODING = 'UTF8'
    LC_COLLATE = 'fr_FR.UTF-8'
    LC_CTYPE = 'fr_FR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    TEMPLATE=template0;

CREATE DATABASE mock
    WITH
    OWNER = champollion
    ENCODING = 'UTF8'
    LC_COLLATE = 'fr_FR.UTF-8'
    LC_CTYPE = 'fr_FR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    TEMPLATE=template0;

CREATE DATABASE test
    WITH
    OWNER = champollion
    ENCODING = 'UTF8'
    LC_COLLATE = 'fr_FR.UTF-8'
    LC_CTYPE = 'fr_FR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    TEMPLATE=template0;
```

### Définition des schémas

Les schémas au sein de chaque base sont gérés par la pipeline de traitement de données (TO DO : mettre lien). Dans la base `champollion`, on retrouve notamment un schéma `public` contenant les données réelles et un schéma `anonymous` contenant des données anonymisées. TO DO : mettre lien vers plus d'explication avec lien vers excel.

## Structure commune

### Conventions

Les conventions suivantes sont respectées :
1. Les noms de tables sont en minuscule, au pluriel, en français, sans articles (du, de, le ...) et sans accents.
2. Les noms de colonnes sont en minuscule, au singulier, en français, sans articles et sans accents.
3. Les préfixes ou suffixes techniques sur les noms des colonnes sont en minuscule et en anglais.

### Types des tables : dynamiques et statiques

La base de données comporte deux types de tables permanentes :
- les **tables dynamiques** qui sont enrichie et mises à jour mensuellement par le flux DSN ;
- les **tables statiques** qui comportent des informations contextuelles.

#### Tables dynamiques

Les tables dynamiques suivent toutes la même structure (à deux exceptions près) :

| Nomenclature              | Fonction                                                        | Type             |
|---------------------------|-----------------------------------------------------------------|------------------|
| `<nom>_id`                 | Id dans la table                                                | `BIGSERIAL`      |
| `<nom_parent>_id`           | Id de l'entité parente dans la table parent                     | `BIGINT`         |
| ...                       |                   (potentiels autres parents)                   | ...              |
| `<nom>_key`                | Clef identifiante de l'entité considérée                      | (sans type fixe) |
| (sans nomenclature)       | Autres données                                                  | (sans type fixe) |
| ...                       |                               ...                               | ...              |
| `date_derniere_declaration` | Date de la dernière déclaration faisant mention de cette entité | `DATE`           |

De plus, elles font état des contraintes suivantes :

- une contrainte d'unicité (`PRIMARY KEY`) sur le champ `<nom>_id` ;
- une contrainte d'unicité (`UNIQUE`) sur le tuple composé de la donnée identifiante `<nom>_key` et du ou des id  `<nom_parent>_id` de son ou ses parent(s) ;
- et une potentielle contrainte de validité (`CHECK`) sur la donnée identiante `<nom>_key` par rapport aux autres informations contenues dans la table (en effet, le champ `<nom>_key` est souvent constitué de la concaténation conditionnelle de plusieurs autres colonnes de la table).

Pour plus d'informations sur les clefs identifiantes, consulter la documentation sur l'intégration des données (TO DO : mettre lien).

#### Tables statiques

Les tables statiques suivent toutes la même structure (à deux exceptions près) :

| Nomenclature | Fonction                      | Type      |
|--------------|-------------------------------|-----------|
| `code_<nom>` | Code à x chiffres             | `CHAR(x)` |
| `libelle`    | Libellé correspondant au code | `VARCHAR` |

### Liste des tables permanentes

Les tables permanentes de la base Champollion sont :

| Table                       | Type      |
|-----------------------------|-----------|
| chargement_donnees          | dynamique |
| entreprises                 | dynamique |
| etablissements              | dynamique |
| salaries                    | dynamique |
| contrats                    | dynamique |
| postes                      | dynamique |
| activites                   | dynamique |
| categories_juridiques_insee | statique  |
| conventions_collectives     | statique  |
| motifs_recours              | statique  |
| naf                         | statique  |
| natures_contrats            | statique  |
| zonage                      | statique  |
| daily_calendar              | statique  |

### Liens entre les tables : clefs étrangères

Les tables de la base sont inter-dépendantes. Pour assurer l'intégrité des données, des clefs étrangères sont utilisées.

<ins>Propriété</ins> : Toute colonne `X.Y_id` d'une table `X` faisant référence à la colonne `id` d'une autre table `Y` est déclarée comme clef étrangère avec :

```sql
ALTER TABLE X ADD FOREIGN KEY (Y_id) REFERENCES Y (id) ON DELETE CASCADE;
```

Cela implique que si une ligne de `X` fait référence à un id spécifique dans sa colonne `X.Y_id`, ce dernier doit exister dans `Y`. On appelle `X.Y_id` la colonne source de la clef étrangère et `Y.id` la colonne cible.

Listes des clefs étrangères en base Champollion : 

| Colonne source                                | Colonne cible                                              |
|-----------------------------------------------|------------------------------------------------------------|
| etablissements.entreprise_id                  | entreprises.entreprise_id                                  |
| salaries.etablissement_id                     | etablissements.etablissement_id                            |
| contrats.salarie_id                           | salaries.salarie_id                                        |
| contrats.etablissement_id                     | etablissements.etablissement_id                            |
| contrats.poste_id                             | postes.poste_id                                            |
| activites.contrat_id                          | contrats.contrat_id                                        |
| contrats.code_motif_recours                   | motifs_recours.code_motif_recours                          |
| contrats.code_convention_collective           | conventions_collectives.code_convention_collective         |
| contrats.code_nature_contrat                  | natures_contrats.code_nature_contrat                       |
| etablissements.code_categorie_juridique_insee | categories_juridiques_insee.code_categorie_juridique_insee |
| etablissements.code_naf                       | naf.code_naf                                               |
| etablissements.code_convention_collective     | conventions_collectives.code_convention_collective         |

### Index

La présence de clefs étrangères dans la base peut engendrer des problèmes de performance. Pour remédier à cela, il est nécessaire d'[indexer](https://docs.postgresql.fr/10/sql-createindex.html) la base.

<ins>Propriété</ins> Toute contrainte d'unicité (`UNIQUE`) déclarée sur une ou plusieurs colonnes implique la création d'un index sur cette ou ces colonne(s).

Malgré, cette propriété on peut rencontrer des problèmes de performance. Par exemple, si on souhaite supprimer des lignes d'une table qui possède une colonne cible d'une clef étrangère alors il est nécessaire d'indexer la colonne source. Pour ce faire, on utilise un `PLAIN` index. A noter qu'un index sur un tuple contenant cette colonne ne sera pas suffisant. Voir [référence](https://dba.stackexchange.com/questions/37034/very-slow-delete-in-postgresql-workaround).

Liste des index en base Champollion : 

| Table                       | Colonne(s) indexée(s)                     | Type de l'index |
|-----------------------------|-------------------------------------------|-----------------|
| entreprises                 | entreprise_id                             | UNIQUE          |
| entreprises                 | entreprise_key                            | UNIQUE          |
| etablissements              | etablissement_id                          | UNIQUE          |
| etablissements              | etablissement_key                         | UNIQUE          |
| salaries                    | salarie_id                                | UNIQUE          |
| salaries                    | etablissement_id, salarie_key             | UNIQUE          |
| contrats                    | contrat_id                                | UNIQUE          |
| contrats                    | etablissement_id, salarie_id, contrat_key | UNIQUE          |
| contrats                    | salarie_id                                | PLAIN           |
| categories_juridiques_insee | code_categorie_juridique_insee            | UNIQUE          |
| conventions_collectives     | code_convention_collective                | UNIQUE          |
| motifs_recours              | code_motif_recours                        | UNIQUE          |
| naf                         | code_naf                                  | UNIQUE          |
| natures_contrats            | code_nature_contrat                       | UNIQUE          |
| activites                   | activite_id                               | UNIQUE          |
| activites                   | contrat_id, mois                          | UNIQUE          |
| postes                      | poste_id                                  | UNIQUE          |
| postes                      | libelle                                   | UNIQUE          |

/!\ L'indexation de colonne peut également avoir des effets négatifs sur les performances. Dès lors, les colonnes ont été indexées seulement lorsque cela était nécessaire pour la bonne exécution des scripts. Si des problèmes de performance venaient à être relevés avec l'ajout de nouveaux sripts, d'autres colonnes pourraient alors être indexées.

## Nature des données

Les informations relatives à la nature des données sont répertoriées dans le catalogue de la base Champollion (TO DO : mettre lien). 

## Trois bases pour quatre cas d'usage

La base `champollion` dans son schéma `public` contient les données réelles et complètes. On décrit dans la suite l'utilité et les caractéristiques des bases `mock` et `test` ainsi que du schéma `anonymous` de la base `champollion`.

### Un schéma de données semi-anonymisées au sein de la base `champollion`

Pour les besoins du développement, un schéma de données semi-anonymisées peut être mis en place automatiquement dans la base `champollion`. Les champs concernés sont les suivants :

- SIREN de l'entreprise
- Raison sociale de l'entreprise
- SIRET de l'établissement
- Enseigne de l'établissement
- Adresse et complément d'adresse de l'établissement
- Nom du salarié
- Nom d'usage du salarié
- Prénoms du salarié
- Date de naissance du salarié
- Lieu de naissance du salarié

A noter que le NIR n'est pas concerné puisque le champ originel est déjà le résultat d'une randomisation.

#### Méthode de semi-anonymisation

La méthode de semi-anonymisation retenue est une anonymisation par glissement : 

> Soit une table $T$ avec une colonne d'id uniques ($c_{id}$) et une colonne de données identifiantes ($c_x$) à anonymiser. Soit $R$ l'ensemble des lignes de $T$. On note $I = \max(\{c_{id}(r)\}_{r \in R})$, l'id maximal de la table. La transformation de semi-anonymisation appliquée est : $\forall r \in R, c_x(r) = c_x(I-r+1)$.

Autrement dit, les données identifiantes d'une ligne sont remplacées par les données identifiantes d'une autre ligne déterminée de manière unique.

A noter que :

- cette transformation est un automorphisme de $[\![1;I]\!]$ dans $[\![1;I]\!]$ mais pas forcément de $\{c_{id}(r)\}_{r \in R}$ dans $\{c_{id}(r)\}_{r \in R}$. Dès lors, si la ligne d'id $I-r+1$ n'existe pas (en base, les id ne sont pas forcément continus), on l'élimine.
- cette méthode de semi-anonymisation n'est pertinente que si l'on ne sélectionne qu'une partie des lignes de la table originelle. Dans le cas contraire, il serait très facile de dé-anonymiser la base.
- si la table $T$ contient plusieurs colonnes identifiantes, le tuple de remplacement correspondra au tuple identifiant d'une seule et même ligne.

#### Schéma `anonymous`

Par souci de simplicité, on ne créé pas une base de données anonymisée mais un schéma `anonymous` dans la base `champollion` existante. Ce dernier est créé à partir du schéma `public` en : 

- sélectionnant les lignes correspondant à une liste établie de SIRET ;
- semi-anonymisant les champs identifiants précédemment cités table par table.

La liste de SIRET sélectionnés est déclarée dans le fichier `champolib/source/data/anonymous_database_selection.csv` (TO DO : mettre lien). A noter que toute entreprise de travail temporaire (ETT) ayant au moins un contrat de travail temporaire avec l'un des établissements listés est ajoutée par défaut.

A noter que la méthode d'anonymisation utilisée ne permet pas de garantir la cohérence des champs anonymisés avec ceux non anonymisés de la même table ainsi que ceux anonymisés d'une table parente. En particulier, on peut avoir :

- une date de naissance dans la table `salaries` postérieure à la date de début d'un contrat dans la table `contrats` ;
- le genre du NIR contradictoire avec le genre présumé à l'aide du prénom ;
- un même individu avec deux noms, prénoms, dates et lieux de naissances différents entre ETT et ETU ;
- un établissement dont l'entreprise parente a un SIREN différent des 9 premiers chiffres de sont SIRET.

Le dernier point ne pose pas de problème à date car le SIREN n'est pas exploité par les *clients* de la base.

#### Comment étendre le périmètre de données ?

TO DO : mettre lien vers pipeline

### Une base de `test` avec des données réelles

La base de test a un objectif double : 
* pouvoir tester ses développements sur une faible volumétrie de données ;
* vérifier le comportement des fonctions d'intégration (tests unitaires des fonctions d'intégration).

Pour ce faire, elle suit la structure commune des bases Champollion mais possède un schéma `test` supplémentaire. Il répertorie les tables contenant les données *attendues*, c'est-à-dire les tables telles qu'elles devraient être si l'intégration des données a un comportement normal. Ces tables sont nomenclaturées avec le préfixe `expected` :

```
.
├── public
    ├── activites
    ├── contrats
    ├── entreprises
    ├── etablissements
    ├── postes
    ├── salaries
    └── ...
└── test
    ├── expected_activites
    ├── expected_contrats
    ├── expected_contrats_comparisons
    ├── expected_entreprises
    ├── expected_etablissements
    ├── expected_postes
    └── expected_salaries
```

Les données intégrées dans cette base sont des données réelles qui ont été sélectionnées afin de tester les scripts d'intégration sur des cas particuliers. Le fichier rassemblant toutes les informations nécessaires à la construction de cette base de test est `source_file_test_data.xlsx` (TO DO : mettre lien). Pour plus d'informations sur la création de cette base `test`, voir TO DO mettre lien pipeline.

La table `expected_contrats_comparisons` sert à connaître le type de comparaison à effectuer en ce qui concerne les champs `contrats.date_fin_effective` et `contrats.statut_fin`. Si `expected_contrats_comparison.date_fin_effective_comparison` est égal à:
* 1 alors la date de fin effective *obtenue* doit être strictement égale à celle *attendue* (idem pour le statut);
* 2, elle doit être supérieure ou égal à celle *attendue* (idem pour le statut);
* ni 1 ni 2, on ne peut pas faire de comparaison (idem pour le statut).

### Une base de fausses données pour le site vitrine

Une base de fausses données remplie à la main a été mise en place. Le fichier permettant la construction de cette dernière est `source_file_mock_data.xlsx` (TO DO : mettre lien). Il comporte un onglet par table. Les colonnes `Id` doivent être cohérentes entre les tables afin d'assurer la construction d'une base pertinente. Cet fichier de référence peut être enrichi par n'importe quel membre de l'équipe projet. A date, aucune fausse donnée n'a été générée pour la table `activites`.

Pour plus d'informations sur la création de cette base `mock`, voir TO DO mettre lien pipeline.