# Introduction

La documentation se décompose en deux parties :

- le dossier [database](TO DO : mettre lien) comporte la documentation technique et fonctionnelle de la base Champollion ;
- le dossier [integration] (TO DO : mettre lien) documente les procédures qui permettent la construction et l'alimentation mensuelle de cette base.

Au sein du dossier [integration](TO DO : mettre lien), la partie [core](TO DO : mettre lien) contient les informations sur les scripts d'intégration de données, quant au dossier [pipeline](TO DO : mettre lien), il explique comment orchestrer ces scripts.

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