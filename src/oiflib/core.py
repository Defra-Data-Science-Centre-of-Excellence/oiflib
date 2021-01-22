"""General functions for oiflib."""

import pandas as pd


def column_name_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """Converts the column names of a DataFrame to string.

    Args:
        df (pd.DataFrame): A DataFrame with column names to convert.

    Returns:
        pd.DataFrame: A DataFrame with column names converted to string.
    """
    df.columns: pd.Index = df.columns.astype(str)
    return df


def index_to_base_year(series: pd.Series) -> pd.Series:
    """Divide each value in a series by the first value, then multiply it by 100.

    Args:
        series ([pd.Series]): [description]

    Returns:
        [pd.Series]: [description]
    """
    return series.div(series.iloc[0]).mul(100)
