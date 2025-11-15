# type: ignore
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "objectstore-client"
copyright = "2025, Sentry"
author = "Sentry"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = []

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
