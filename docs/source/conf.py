import os
import sys

sys.path.insert(0, os.path.abspath('..'))

extensions = [
    'sphinx.ext.napoleon',
    'sphinxcontrib.asyncio',
]

master_doc = 'index'

project = 'qpid-bow'
copyright = '2018, Bynder B.V.'
author = 'Bynder B.V.'

version = '1.0'  # The short X.Y version.
release = '1.0.2'  # The full version, including alpha/beta/rc tags.

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

pygments_style = 'sphinx'
html_theme = 'sphinx_rtd_theme'

napoleon_numpy_docstring = False
