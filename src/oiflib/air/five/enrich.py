"""Functions for enriching Air Five DataFrame."""

from pandas import DataFrame


def _add_bounds(df: DataFrame) -> DataFrame:
    """Adds upper and lower bound columns.

    Args:
        df (DataFrame): The extracted Air Five DataFrame.

    Returns:
        DataFrame: An enriched DataFrame with upper and lower bound columns.
    """
    return df.assign(
        Lower_CI_bound=df["Annual Mean NO2 concentration (µg/m3)"]
        - df["95% confidence interval for 'All sites' (+/-)"],
        Upper_CI_bound=df["Annual Mean NO2 concentration (µg/m3)"]
        + df["95% confidence interval for 'All sites' (+/-)"],
    )


def enrich_air_five(df: DataFrame) -> DataFrame:
    """Adds upper and lower bound columns.

    Args:
        df (DataFrame): The extracted Air Five DataFrame.

    Returns:
        DataFrame: An enriched DataFrame with upper and lower bound columns.
    """
    return df.pipe(_add_bounds)
