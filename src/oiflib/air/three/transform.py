"""Functions to transform Air Three DataFrames."""

import pandas as pd


def unpivot(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.melt(
        id_vars=["Area code", "Country"],
        value_name="ugm-3",
    )


def split_variable_column(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    df["year"] = df.variable.str.extract(r"(\d{4})")

    df["measure"] = df.variable.str.extract(r"\((\S*)\)")

    return df


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df[["Area code", "Country", "year", "measure", "ugm-3"]]


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.query('Country == "England" & measure == "total"')


def transform_air_three(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return (
        df.pipe(unpivot)
        .pipe(split_variable_column)
        .pipe(select_columns)
        .pipe(filter_rows)
    )
