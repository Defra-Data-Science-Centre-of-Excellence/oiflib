"""Functions to push data to OpenSDG data repo."""
from typing import Optional, Tuple, Union

from git import Repo
from pandas import DataFrame

from oiflib._helper import _oiflib_to_sdg_lookup


def _reset_local_branch(
    root: str,
    repo: str,
    branches: Union[str, Tuple[str, str]],
) -> None:
    """[summary].

    Args:
        root (str): [description]
        repo (str): [description]
        branches (Union[str, Tuple[str, str]]): [description]

    Raises:
        Exception: If the GitPython commands raise an exception.
    """
    _branch: str

    if isinstance(branches, tuple):
        _branch = branches[1]
    else:
        _branch = branches

    _repo: Repo = Repo(f"{root}/{repo}")

    try:
        _repo.remotes.origin.fetch()
        _repo.head.reset(commit=f"origin/{_branch}")
    except Exception:
        raise


def _set_data_file_name(
    theme: str,
    indicator: str,
) -> str:
    """Given theme and indicator, sets OpenSDG-style filename.

    Example:
        >>> file_name = _set_data_file_name(
            theme="air",
            indicator="one",
        )
        indicator_1-1-1.csv

    Args:
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".

    Returns:
        str: OpenSDG-style filename as string.
    """
    oiflib_name: str = f"{theme}_{indicator}"
    return f"indicator_{_oiflib_to_sdg_lookup.get(oiflib_name)}.csv"


def _write_to_csv(
    df: DataFrame,
    root: str,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> str:
    """[summary].

    Args:
        df (DataFrame): [description]
        root (str): [description]
        repo (str): [description]
        data_folder (str): [description]
        data_file_name (str): [description]

    Returns:
        str: [description]
    """
    file_path: str = f"{root}/{repo}/{data_folder}/{data_file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )

    return file_path


def _add(
    root: str,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> None:
    """[summary].

    Args:
        root (str): [description]
        repo (str): [description]
        data_folder (str): [description]
        data_file_name (str): [description]

    Returns:
        [type]: [description]
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    _repo.index.add(f"{data_folder}/{data_file_name}")

    return None


def _commit(
    root: str,
    repo: str,
    theme: str,
    indicator: str,
    data_commit_message: Optional[str],
) -> None:
    """[summary].

    Args:
        root (str): [description]
        repo (str): [description]
        theme (str): [description]
        indicator (str): [description]
        data_commit_message (Optional[str]): [description]

    Returns:
        [type]: [description]
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    if not data_commit_message:
        data_commit_message = f"add data for {theme} {indicator}"

    _repo.index.commit(data_commit_message)

    return None


def _push(
    root: str,
    repo: str,
    branches: Union[str, Tuple[str, str]],
) -> None:
    """[summary].

    Args:
        root (str): [description]
        repo (str): [description]
        branches (Union[str, Tuple[str, str]]): [description]

    Returns:
        [type]: [description]
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    _refspec: str

    if isinstance(branches, tuple):
        _refspec = f"{branches[0]}:{branches[1]}"
    else:
        _refspec = f"{branches}:{branches}"

    _repo.remotes.origin.push(refspec=_refspec)

    return None


def publish(
    df: DataFrame,
    theme: str,
    indicator: str,
    branches: Union[str, Tuple[str, str]] = "develop",
    root: str = ".",
    repo: str = "OIF-Dashboard-Data",
    data_folder: str = "data",
    data_commit_message: Optional[str] = None,
) -> None:
    """[summary].

    Args:
        df (DataFrame): [description]
        theme (str): [description]
        indicator (str): [description]
        branches (Union[str, Tuple[str, str]], optional): [description]. Defaults to
            "develop".
        root (str): [description]. Defaults to ".".
        repo (str): [description]. Defaults to "OIF-Dashboard-Data".
        data_folder (str): [description]. Defaults to "data".
        data_commit_message (Optional[str]): [description]. Defaults to None.
    """
    _data_file_name: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    _write_to_csv(
        df=df,
        root=root,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
    )

    _add(
        root=root,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
    )

    _commit(
        root=root,
        repo=repo,
        theme=theme,
        indicator=indicator,
        data_commit_message=data_commit_message,
    )

    _push(
        root=root,
        repo=repo,
        branches=branches,
    )
