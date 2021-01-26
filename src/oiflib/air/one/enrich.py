"""Functions to enrich transformed Air One data."""

from pandas import DataFrame

from oiflib.core import index_to_base_year


def index_emission_to_base_year(df: DataFrame) -> DataFrame:
    """Add "Index" column with "Emission" values indexed to base year.

    Args:
        df (DataFrame): A DataFrame with an "Emissions" column to index.

    Returns:
        DataFrame: A Dataframe with an "Index" column.
    """
    return df.assign(
        Index=(df.groupby("ShortPollName").Emission.apply(index_to_base_year))
    )


def enrich_air_one(df: DataFrame) -> DataFrame:
    """Enrich transformed Air One data.

    This function should be applied to a transformed and validated Air One DataFrame.
    It calls the function index_emissions_to_base_year() on the input DataFrame, which
    adds an "Index" column with "Emission" values indexed to base year for each
    pollutant.

    Args:
        df (DataFrame): A transformed and validated Air One DataFrame.

    Returns:
        DataFrame: An enriched Dataframe with an "Index" column.
    """
    return df.pipe(index_emission_to_base_year)
