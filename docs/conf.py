# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DSN processing"
copyright = "2023, Margot COSSON"
author = "Margot COSSON, Léo GUILLAUME, Yan ZHI"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["myst_parser"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

language = "en"

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "globaltoc_collapse": True,
    "globaltoc_maxdepth": -1,
    "body_max_width": "none",
}
html_static_path = ["_static"]
html_css_files = [
    "avoid_rolling_tab.css",
    "increase_width.css",
]
myst_heading_anchors = 5
myst_enable_extensions = [
    "dollarmath",
]
