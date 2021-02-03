"""TODO module docstring."""

import pandas as pd


def add_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.assign(
        Lower_CI_bound=df[
            "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"
        ]
        - df["95% confidence interval for 'All sites' (+/-)"],
        Upper_CI_bound=df[
            "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"
        ]
        + df["95% confidence interval for 'All sites' (+/-)"],
    )


def enrich_air_four(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df.pipe(add_bounds)
