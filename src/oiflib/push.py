"""Functions to push data to OpenSDG data repo."""
from typing import List, Optional, Union

from git import Repo
from pandas import DataFrame

from oiflib._helper import indicator_lookup, theme_lookup


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
    return f"indicator_{getattr(theme_lookup, theme)}-{getattr(indicator_lookup, indicator)}-1.csv"  # noqa: B950 - breaking the line would decrease readability


def _write_to_csv(
    df: DataFrame,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> None:
    """#TODO [summary].

    Args:
        df (DataFrame): #TODO [description]
        repo (str): #TODO [description]
        data_folder (str): #TODO [description]
        data_file_name (str): #TODO [description]
    """
    file_path: str = f"{repo}/{data_folder}/{data_file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )


def _commit_and_push(
    repo: str,
    data_folder: str,
    data_file_name: str,
    theme: str,
    indicator: str,
    branches: Union[str, List[str]],
    data_commit_message: Optional[str],
) -> None:
    """#TODO [summary].

    Args:
        repo (str): #TODO [description]
        data_folder (str): #TODO [description]
        data_file_name (str): #TODO [description]
        theme (str): #TODO [description]
        indicator (str): #TODO [description]
        branches (Union[str, List[str]]): #TODO [description]
        data_commit_message (Optional[str]): #TODO [description]
    """
    _repo: Repo = Repo(repo)

    if data_folder and data_file_name:

        _repo.index.add(f"{data_folder}/{data_file_name}")

        if not data_commit_message:
            data_commit_message = f"add data for {theme} {indicator}"

        _repo.index.commit(data_commit_message)

    _refspec: str

    if isinstance(branches, list):
        _refspec = f"{branches[0]}:{branches[1]}"
    else:
        _refspec = f"{branches}:{branches}"

    _repo.remotes.origin.push(refspec=_refspec)


def push(
    df: DataFrame,
    theme: str,
    indicator: str,
    branches: Union[str, List[str]] = "develop",
    data_commit_message: Optional[str] = None,
    repo: str = "OIF-Dashboard-Data",
    data_folder: str = "data",
) -> None:
    """[summary].

    >>> push(
        df=air_one_formatted_validated,
        theme="air",
        indicator="one",
    )

    Args:
        df (DataFrame): [description]
        theme (str): [description]
        indicator (str): [description]
        branches (Union[str, List[str]], optional): [description]. Defaults to
            "develop".
        data_commit_message (Optional[str], optional): [description]. Defaults to None.
        repo (str): [description]. Defaults to "OIF-Dashboard-Data".
        data_folder (str): [description]. Defaults to "data".
    """
    _data_file_name: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    _write_to_csv(
        df=df,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
    )

    _commit_and_push(
        theme=theme,
        indicator=indicator,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
        branches=branches,
        data_commit_message=data_commit_message,
    )
