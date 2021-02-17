Development
===========

Repository structure
--------------------

.github/workflows
^^^^^^^^^^^^^^^^^

This folder contains the GitHub Actions configuration files. See
`Continuous Integration`_ for more information.

data
^^^^

This folder contains data files, they will be moving to online storage shortly.

docs
^^^^

This folder contains the files used to create the library documentation. See
`Project Documentation`_ for more information.

scr/oiflib
^^^^^^^^^^

This folder contains the library source code. See `Hynek Schlawack, Testing & Packaging`_
for more information about why it uses a src package layout.

.. _Hynek Schlawack, Testing & Packaging: https://hynek.me/articles/testing-packaging/

tests
^^^^^

This folder contains unit tests and associated configuration. See Testing_ for more
information.

Top-level files
^^^^^^^^^^^^^^^

.darglint
"""""""""

The configuration file for darglint. See Linting_ for more information.

.flake8
"""""""

The configuration file for flake8. See Linting_ for more information.

.gitignore
""""""""""

A GitHub configuration file. It includes a list of rules for ignoring files. See
`Ignoring files`_ for more information.

.. _Ignoring files: https://docs.github.com/en/github/using-git/ignoring-files

.pre-commit-config.yaml
"""""""""""""""""""""""

The configuration file for pre-commit. See `Continuous Integration`_ for more information.

.python-version
"""""""""""""""

The configuration file for pyenv. See `Version and Dependency Management`_ for more
information.

.readthedocs.yaml
"""""""""""""""""

The configuration file for Read the Docs. See `Project Documentation`_ for more information.

LICENSE.md
""""""""""

Software license file. See `Licensing a repository`_ for more information.

.. _Licensing a repository: https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/licensing-a-repository

README.md
"""""""""

A text file containing information that is displayed on the repoistory home page.

mypy.ini
""""""""

The configuration file for mypy. See `Static Type Checking`_ for more information.

noxfile.py
""""""""""

The configuration file for nox. See `Continuous Integration`_ for more information.

poetry.lock
"""""""""""

A list of dependencies created by poetry. See `Version and Dependency Management`_ for more
information.

pyproject.toml
""""""""""""""

The overall library configuration file. See `Version and Dependency Management`_ for more
information.

Tools and practices
-------------------

This project implements many of the tools and best practices recommended in `Hypermodern Python`_.

.. _Hypermodern Python: https://cjolowicz.github.io/posts/hypermodern-python-01-setup/

Visual Studio Code
^^^^^^^^^^^^^^^^^^

This library has been developed using the `Visual Studio Code`_ IDE.

.. _Visual Studio Code: https://code.visualstudio.com/

The following extensions have been very useful:

- aaron-bond.better-comments
- github.vscode-pull-request-github
- ms-python.python
- ms-python.vscode-pylance
- ms-toolsai.jupyter
- ms-vscode-remote.vscode-remote-extensionpack
- njpwerner.autodocstring
- streetsidesoftware.code-spell-checker

.. _version-and-dependency-management-label:

Version and Dependency Management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- `pyenv <https://github.com/pyenv/pyenv>`_ for Python version management.
- poetry_ is a tool for dependency management and packaging.
  It is recommended by the `Python Packaging Authority`_ as an alternative to pipenv_.

.. _poetry: https://python-poetry.org/

.. _Python Packaging Authority: https://packaging.python.org/guides/tool-recommendations/

.. _pipenv: https://pipenv.pypa.io/en/latest/

Testing
^^^^^^^

- `pytest <https://docs.pytest.org/en/latest/>`_ for unit testing.
    - with `coverage <https://coverage.readthedocs.io/>`_ to measure test coverage
- Property-based tests using `hypothesis <https://hypothesis.readthedocs.io/en/latest/>`_.
- `mocks <https://towardsdatascience.com/stop-mocking-me-unit-tests-in-pyspark-using-pythons-mock-library-a4b5cd019d7e>`_, `fakes, stubs <https://blog.pragmatists.com/test-doubles-fakes-mocks-and-stubs-1a7491dfa3da>`_, or `markers <https://docs.pytest.org/en/latest/example/markers.html>`_ in testing

Formatting
^^^^^^^^^^

- `black <https://github.com/psf/black>`_ for formatting.
- `isort <https://timothycrosley.github.io/isort/>`_ to order imports based on the `Google styleguide <https://google.github.io/styleguide/pyguide.html?showone=Imports_formatting#313-imports-formatting>`_.

Static Type Checking
^^^^^^^^^^^^^^^^^^^^

- The project uses `type hints as per PEP 484 <https://www.python.org/dev/peps/pep-0484/>`_
- `mypy <http://mypy-lang.org/>`_ for static type checking

Documentation
^^^^^^^^^^^^^

Module and Function Docstrings
""""""""""""""""""""""""""""""

- `Google style <https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings>`_ `Docstrings <https://www.python.org/dev/peps/pep-0257/#what-is-a-docstring>`_

Project Documentation
"""""""""""""""""""""

- `sphinx <http://www.sphinx-doc.org/>`_ to generate documentation, with the following extentsions:
    - `autodoc <https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html>`_ to automatically generate documentation from docstrings
    - `napoleon <https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html>`_ to convert google-style docstrings to reStructuredText
    - sphinx-autobuild
- `readthedocs <https://readthedocs.org/>`_ to host the documentation generated by sphinx

Linting
^^^^^^^

- `flake8 <https://flake8.pycqa.org/en/latest/>`_ for linting, with the following plugins:
    - `flake8-black <https://github.com/peterjc/flake8-black>`_ to ensure alignment between linting and formatting tools
    - `flake8-isort <https://github.com/gforcada/flake8-isort>`_ to ensure alignment between linting and import ordering tools
    - `flake8-bugbear <https://github.com/PyCQA/flake8-bugbear>`_ to highlight various bugs and design issues not captured by other linters
    - `flake8-bandit <https://github.com/tylerwince/flake8-bandit>`_ to identify security issues
    - `flake8-annotation <https://github.com/python-discord/flake8-annotations>`_ to check for missing type hints
    - `flake8-docstrings <https://gitlab.com/pycqa/flake8-docstrings>`_ to check docstring style compliance
    - `darglint <https://github.com/terrencepreilly/darglint>`_ to check that docstring descriptions match function definitions

Continuous Integration
^^^^^^^^^^^^^^^^^^^^^^

- `Nox <https://nox.thea.codes/>`_ for test automation
- `GitHub Actions <https://docs.github.com/en/actions>`_ for continuous integration
- `pre-commit <https://pre-commit.com/>`_ to leverage linters written in other languages


Security
^^^^^^^^

- `Saftey <https://github.com/pyupio/safety>`_ to identify security vulnerabilities


Not yet Implemented
^^^^^^^^^^^^^^^^^^^

It doesn't currently use:

- `pytype <https://google.github.io/pytype/>`_ for static type checking
- `typeguard <https://github.com/agronholm/typeguard>`_ for runtime type checking
- `Desert <https://desert.readthedocs.io/>`_ or `Marshmallow <https://marshmallow.readthedocs.io/>`_ for data validation
- `xdoctest <https://github.com/Erotemic/xdoctest>`_ to test docstring examples
- `sphinx-autodoc-typehints <https://github.com/agronholm/sphinx-autodoc-typehints>`_ to include type hints in documentation
- `codecov <https://codecov.io/>`_ to report testing coverage
- `pypi <https://pypi.org/>`_ for package hosting or `testpypi <https://test.pypi.org/>`_ to test package hosting
- `release-drafter <https://github.com/release-drafter/release-drafter>`_ to help draft release notes
- `Semantic Versioning <https://semver.org/>`_ to indicate breaking changes, minor changes, or bug fixes
