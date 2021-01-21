"""TODO module docstring."""

import pandas as pd


def forward_fill_column(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    df.NCFormat: pd.Series = df.NCFormat.ffill()

    return df


def unpivot(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.melt(
        id_vars=["NCFormat", "IPCC"],
        var_name="Year",
        value_name="GWP_CO2_AR4",
    )


def transform_air_two(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.pipe(forward_fill_column).pipe(unpivot)
