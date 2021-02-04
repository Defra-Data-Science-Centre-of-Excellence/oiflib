"""Functions for enriching Air Four DataFrame."""

from pandas import DataFrame


def _add_bounds(df: DataFrame) -> DataFrame:
    """Adds upper and lower bound columns.

    Args:
        df (DataFrame): The extracted Air Four DataFrame.

    Returns:
        DataFrame: An enriched DataFrame with upper and lower bound columns.
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


def enrich_air_four(df: DataFrame) -> DataFrame:
    """Adds upper and lower bound columns.

    Args:
        df (DataFrame): The extracted Air Four DataFrame.

    Returns:
        DataFrame: An enriched DataFrame with upper and lower bound columns.
    """
    return df.pipe(_add_bounds)
