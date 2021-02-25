"""Functions to format and push data to OpenSDG data repo."""
from typing import Dict, List, Optional, Union

from git import Repo
from pandas import DataFrame

theme_lookup: Dict[str, str] = {
    "air": "1",
}

indicator_lookup: Dict[str, str] = {
    "one": "1",
    "two": "2",
}


def _select_and_rename(
    df: DataFrame,
    year_column: str,
    disaggregation_column: str,
    value_column: str,
    disaggregation_column_new: Optional[str],
) -> DataFrame:
    """Selects and renames columns to fit OpenSDG dataset format.

    The OpenSDG platform expects the first column to be "Year", the last to be "Value"
    and any in between to be disaggregations. This function re-orders and re-names the
    columns of a given DataFrame accordingly. It also gives the caller the opportunity
    to rename the disaggregation column.

    >>> _select_and_rename(
        df=air_one_enriched_validated,
        year_column="EmissionYear",
        disaggregation_column="ShortPollName",
        disaggregation_column_new="Pollutant",
        value_column="Index",
    )

    Args:
        df (DataFrame): A DataFrame will columns to be re-order and re-named.
        year_column (str): The name of the column containing year data.
        disaggregation_column (str): The name of the column containing disaggregation
            data.
        value_column (str): The name of the column containing value data.
        disaggregation_column_new (Optional[str], optional): A new name for the column
            containing disaggregation data, if one is needed. Defaults to None.

    Returns:
        DataFrame: A DataFrame with re-ordered and re-named columns.
    """
    columns: Dict[str, str]

    if not disaggregation_column_new:
        columns = {
            year_column: "Year",
            disaggregation_column: disaggregation_column,
            value_column: "Value",
        }
    else:
        columns = {
            year_column: "Year",
            disaggregation_column: disaggregation_column_new,
            value_column: "Value",
        }

    return df[columns.keys()].rename(columns=columns)


def _set_file_name(
    theme: str,
    indicator: str,
) -> str:
    """[summary].

    Args:
        theme (str): [description]
        indicator (str): [description]

    Returns:
        str: [description]
    """
    return f"indicator_{theme_lookup.get(theme)}-{indicator_lookup.get(indicator)}-1.csv"  # noqa: B950 - breaking the line would decrease readability


def _write_to_csv(
    df: DataFrame,
    repo: str,
    folder: str,
    file_name: str,
) -> None:
    """[summary].

    Args:
        df (DataFrame): [description]
        repo (str): [description]
        folder (str): [description]
        file_name (str): [description]
    """
    file_path: str = f"{repo}/{folder}/{file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )


def _commit_and_push(
    repo: str,
    folder: str,
    file_name: str,
    theme: str,
    indicator: str,
    branches: Union[str, List[str]],
    commit_message: Optional[str],
) -> None:
    """[summary].

    Args:
        repo (str): [description]
        folder (str): [description]
        file_name (str): [description]
        theme (str): [description]
        indicator (str): [description]
        branches (Union[str, List[str]]): [description]
        commit_message (Optional[str]): [description]
    """
    _repo: Repo = Repo(repo)

    _repo.index.add(f"{folder}/{file_name}")

    if not commit_message:
        commit_message = f"add data for {theme} {indicator}"

    _repo.index.commit(commit_message)

    _refspec: str

    if isinstance(branches, list):
        _refspec = f"{branches[0]}:{branches[1]}"
    else:
        _refspec = f"{branches}:{branches}"

    _repo.remotes.origin.push(refspec=_refspec)


def format_and_push(
    df: DataFrame,
    theme: str,
    indicator: str,
    year_column: str,
    disaggregation_column: str,
    value_column: str,
    branches: Union[str, List[str]] = "develop",
    commit_message: Optional[str] = None,
    disaggregation_column_new: Optional[str] = None,
    repo: str = "OIF-Dashboard-Data",
    folder: str = "data",
) -> None:
    """[summary].

    >>> format_and_push(
        df=air_one_enriched_validated,
        theme="air",
        indicator="one",
        year_column="EmissionYear",
        disaggregation_column="ShortPollName",
        disaggregation_column_new="Pollutant",
        value_column="Index",
    )

    Args:
        df (DataFrame): [description]
        theme (str): [description]
        indicator (str): [description]
        year_column (str): [description]
        disaggregation_column (str): [description]
        value_column (str): [description]
        branches (Union[str, List[str]]): [description]. Defaults to "develop".
        commit_message (Optional[str], optional): [description]. Defaults to None.
        disaggregation_column_new (Optional[str], optional): [description]. Defaults
            to None.
        repo (str): [description]. Defaults to "OIF-Dashboard-Data".
        folder (str): [description]. Defaults to "data".
    """
    _df: DataFrame = _select_and_rename(
        df=df,
        year_column=year_column,
        disaggregation_column=disaggregation_column,
        value_column=value_column,
        disaggregation_column_new=disaggregation_column_new,
    )

    _file_name: str = _set_file_name(
        theme=theme,
        indicator=indicator,
    )

    _write_to_csv(
        df=_df,
        repo=repo,
        folder=folder,
        file_name=_file_name,
    )

    _commit_and_push(
        theme=theme,
        indicator=indicator,
        repo=repo,
        folder=folder,
        file_name=_file_name,
        branches=branches,
        commit_message=commit_message,
    )
