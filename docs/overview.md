# Introduction

La documentation se décompose en deux parties :

- le dossier [database](./database/index.rst) comporte la documentation technique et fonctionnelle de la base Champollion ;
- le dossier [integration](./integration/index.rst) documente les procédures qui permettent la construction et l'alimentation mensuelle de cette base.

Au sein du dossier [integration](./integration/index.rst), la partie [core](./integration/core/index.rst) contient les informations sur les scripts d'intégration de données, quant au dossier [pipeline](./integration/pipeline/index.rst), il explique comment orchestrer ces scripts.

*Génération de la documentation HTML*

On génère les fichiers du site statique de documentation à partir des fichiers markdown grâce à [Sphinx](https://www.sphinx-doc.org/en/master/index.html).

1. S'assurer que `sphinx`, `myst-parser` et `sphinx-rtd-theme` sont bien installés :

```bash
pip show sphinx
pip show myst-parser
pip show sphinx-rtd-theme
```

2. Compiler les fichiers HTML

```bash
cd dsn_processing/docs
make html
```

3. Visualiser le site statique sur le port XXXX

```bash
cd dsn_processing/docs/_build/html
python -m http.server XXXX
```