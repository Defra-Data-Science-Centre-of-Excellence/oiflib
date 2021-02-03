"""Nox sessions."""
import tempfile
from typing import Any

import nox
from nox.sessions import Session

package = "oiflib"
nox.options.sessions = "isort", "lint", "safety", "mypy", "tests"
locations = [
    "noxfile.py",
    "src/oiflib/__init__.py",
    "src/oiflib/extract.py",
    "src/oiflib/core.py",
    "src/oiflib/air/two/transform.py",
    "src/oiflib/air/two/__init__.py",
    "src/oiflib/air/two/schemas.py",
    "src/oiflib/air/two/enrich.py",
    "src/oiflib/air/two/validate.py",
    "src/oiflib/air/three/transform.py",
    "src/oiflib/air/three/__init__.py",
    "src/oiflib/air/three/schemas.py",
    "src/oiflib/air/three/enrich.py",
    "src/oiflib/air/three/extract.py",
    "src/oiflib/air/three/validate.py",
    "src/oiflib/air/one/transform.py",
    "src/oiflib/air/one/__init__.py",
    "src/oiflib/air/one/schemas.py",
    "src/oiflib/air/one/enrich.py",
    "src/oiflib/air/one/validate.py",
    "data/air/two/_lookup.py",
    "tests/__init__.py",
    "tests/test_extract.py",
    "tests/test_oiflib.py",
    "tests/test_core.py",
    "tests/air/__init__.py",
    "tests/air/two/test_schemas.py",
    "tests/air/two/__init__.py",
    "tests/air/two/test_enrich.py",
    "tests/air/two/test_transform.py",
    "tests/air/two/test_validate.py",
    "tests/air/one/__init__.py",
    "tests/air/one/test_enrich.py",
    "tests/air/one/test_transform.py",
    "docs/source/conf.py",
]


def install_with_constraints(session: Session, *args: str, **kwargs: Any) -> None:
    """Install packages constrained by Poetry's lock file.

    This function is a wrapper for nox.sessions.Session.install. It
    invokes pip to install packages inside of the session's virtualenv.
    Additionally, pip is passed a constraints file generated from
    Poetry's lock file, to ensure that the packages are pinned to the
    versions specified in poetry.lock. This allows you to manage the
    packages as Poetry development dependencies.

    Arguments:
        session: The Session object.
        args: Command-line arguments for pip.
        kwargs: Additional keyword arguments for Session.install.

    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python="3.8")
def isort(session: Session) -> None:
    """Sort imports with isort."""
    args = session.posargs or locations
    install_with_constraints(session, "isort")
    session.run("isort", *args)


@nox.session(python="3.8")
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python="3.8")
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    install_with_constraints(
        session,
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
        "darglint",
    )
    session.run("flake8", *args)


@nox.session(python="3.8")
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run(
            "safety",
            "check",
            f"--file={requirements.name}",
            "--full-report",
            "--ignore=39462",
        )  # ignore tornado vulnerability CVE-2020-28476


@nox.session(python="3.8")
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    install_with_constraints(session, "mypy")
    session.run("mypy", *args, "--ignore-missing-imports")


@nox.session(python="3.8")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(
        session,
        "coverage[toml]",
        "pytest",
        "pytest-cov",
        "hypothesis",
    )
    session.run("pytest", *args)


@nox.session(python="3.8")
def docs(session: Session) -> None:
    """Build the documentation."""
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(session, "sphinx", "nbsphinx")
    session.run("sphinx-build", "docs", "docs/_build")
