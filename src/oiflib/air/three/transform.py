"""Functions to transform Air Three DataFrames."""

from pandas import DataFrame


def unpivot(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return df.melt(
        id_vars=["Area code", "Country"],
        value_name="ugm-3",
    )


def split_variable_column(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    df["year"] = df.variable.str.extract(r"(\d{4})")

    df["measure"] = df.variable.str.extract(r"\((\S*)\)")

    return df


def select_columns(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return df[["Area code", "Country", "year", "measure", "ugm-3"]]


def filter_rows(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return df.query('Country == "England" & measure == "total"')


def transform_air_three(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return (
        df.pipe(unpivot)
        .pipe(split_variable_column)
        .pipe(select_columns)
        .pipe(filter_rows)
    )
