"""Functions to format and push data to OpenSDG data repo."""
from typing import Dict, List, Optional, Union

from git import Repo
from pandas import DataFrame

from oiflib._helper import indicator_lookup, theme_lookup
from oiflib.metadata import _set_meta_file_name


def _select_and_rename(
    df: DataFrame,
    year_column: str,
    value_column: str,
    disaggregation_column: Optional[str],
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
        value_column="Index",
        disaggregation_column="ShortPollName",
        disaggregation_column_new="Pollutant",
    )

    Args:
        df (DataFrame): A DataFrame will columns to be re-order and re-named.
        year_column (str): The name of the column containing year data.
        value_column (str): The name of the column containing value data.
        disaggregation_column (Optional[str]): The name of the column containing
            disaggregation data, if one exists.
        disaggregation_column_new (Optional[str]): A new name for the column
            containing disaggregation data, if one is needed.

    Returns:
        DataFrame: A DataFrame with re-ordered and re-named columns.
    """
    columns: Dict[str, str]

    if not disaggregation_column:
        columns = {
            year_column: "Year",
            value_column: "Value",
        }
    elif not disaggregation_column_new:
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


def _set_data_file_name(
    theme: str,
    indicator: str,
) -> str:
    """#TODO [summary].

    Args:
        theme (str): #TODO [description]
        indicator (str): #TODO [description]

    Returns:
        str: #TODO [description]
    """
    return f"indicator_{theme_lookup.get(theme)}-{indicator_lookup.get(indicator)}-1.csv"  # noqa: B950 - breaking the line would decrease readability


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
    meta_folder: str,
    meta_file_name: str,
    theme: str,
    indicator: str,
    branches: Union[str, List[str]],
    data_commit_message: Optional[str],
    meta_commit_message: Optional[str],
) -> None:
    """#TODO [summary].

    Args:
        repo (str): #TODO [description]
        data_folder (str): #TODO [description]
        data_file_name (str): #TODO [description]
        meta_folder (str): #TODO [description]
        meta_file_name (str): #TODO [description]
        theme (str): #TODO [description]
        indicator (str): #TODO [description]
        branches (Union[str, List[str]]): #TODO [description]
        data_commit_message (Optional[str]): #TODO [description]
        meta_commit_message (Optional[str]): #TODO [description]
    """
    _repo: Repo = Repo(repo)

    if data_folder and data_file_name:

        _repo.index.add(f"{data_folder}/{data_file_name}")

        if not data_commit_message:
            data_commit_message = f"add data for {theme} {indicator}"

        _repo.index.commit(data_commit_message)

    if meta_folder and meta_file_name:

        _repo.index.add(f"{meta_folder}/{meta_file_name}")

        if not meta_commit_message:
            meta_commit_message = f"add meta for {theme} {indicator}"

        _repo.index.commit(meta_commit_message)

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
    value_column: str,
    branches: Union[str, List[str]] = "develop",
    data_commit_message: Optional[str] = None,
    meta_commit_message: Optional[str] = None,
    disaggregation_column: Optional[str] = None,
    disaggregation_column_new: Optional[str] = None,
    repo: str = "OIF-Dashboard-Data",
    data_folder: str = "data",
    meta_folder: str = "meta",
) -> None:
    """[summary].

    >>> format_and_push(
        df=air_one_enriched_validated,
        theme="air",
        indicator="one",
        year_column="EmissionYear",
        value_column="Index",
        disaggregation_column="ShortPollName",
        disaggregation_column_new="Pollutant",
    )

    Args:
        df (DataFrame): [description]
        theme (str): [description]
        indicator (str): [description]
        year_column (str): [description]
        value_column (str): [description]
        branches (Union[str, List[str]], optional): [description]. Defaults to
            "develop".
        data_commit_message (Optional[str], optional): [description]. Defaults to None.
        meta_commit_message (Optional[str], optional): [description]. Defaults to None.
        disaggregation_column (Optional[str]): [description]. Defaults to None.
        disaggregation_column_new (Optional[str], optional): [description]. Defaults to
            None.
        repo (str): [description]. Defaults to "OIF-Dashboard-Data".
        data_folder (str): [description]. Defaults to "data".
        meta_folder (str): [description]. Defaults to "meta".
    """
    _df: DataFrame = _select_and_rename(
        df=df,
        year_column=year_column,
        disaggregation_column=disaggregation_column,
        value_column=value_column,
        disaggregation_column_new=disaggregation_column_new,
    )

    _data_file_name: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    _meta_file_name: str = _set_meta_file_name(
        theme=theme,
        indicator=indicator,
    )

    _write_to_csv(
        df=_df,
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
        meta_folder=meta_folder,
        meta_file_name=_meta_file_name,
        branches=branches,
        data_commit_message=data_commit_message,
        meta_commit_message=meta_commit_message,
    )
