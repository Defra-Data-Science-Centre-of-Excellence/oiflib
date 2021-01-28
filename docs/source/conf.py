"""Configuration file for the Sphinx documentation builder."""

# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from typing import Any, List

import sphinx_rtd_theme  # noqa: F401 - It is being used

sys.path.insert(0, os.path.abspath(os.path.join("..", "..")))

# -- Project information -----------------------------------------------------

project: str = "oiflib"
copyright: str = "2021, Ed Fawcett-Taylor"
author: str = "Ed Fawcett-Taylor"

# The full version, including alpha/beta/rc tags
release: str = "alpha"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions: List[str] = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_rtd_theme",
]

# Napoleon settings
napoleon_google_docstring: bool = True
napoleon_numpy_docstring: bool = False
napoleon_include_init_with_doc: bool = False
napoleon_include_private_with_doc: bool = False
napoleon_include_special_with_doc: bool = True
napoleon_use_admonition_for_examples: bool = False
napoleon_use_admonition_for_notes: bool = False
napoleon_use_admonition_for_references: bool = False
napoleon_use_ivar: bool = False
napoleon_use_param: bool = True
napoleon_use_rtype: bool = True
napoleon_type_aliases: Any = None
napoleon_attr_annotations: bool = True

# Add any paths that contain templates here, relative to this directory.
templates_path: List[str] = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns: List[str] = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme: str = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path: List[str] = ["_static"]
