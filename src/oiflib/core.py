"""Core functions for oiflib."""

import pandas as pd


def index_to_base_year(series: pd.Series) -> pd.Series:
    """Divide each value in a series by the first value, then multiply it by 100.

    Args:
        series ([pd.Series]): [description]

    Returns:
        [pd.Series]: [description]
    """
    return series.div(series.iloc[0]).mul(100)
