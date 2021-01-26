"""Functions to enrich the air two data."""

from pandas import DataFrame, merge, read_csv


def join_to_lookup(df: DataFrame) -> DataFrame:
    """Join to "NCFormat" + "IPCC" to "OIF_category" lookup table.

    Args:
        df (DataFrame): A DataFrame to join.

    Returns:
        DataFrame: A DataFrame with "OIF_category" column.
    """
    df_lookup: DataFrame = read_csv(
        filepath_or_buffer="/home/edfawcetttaylor/repos/oiflib/data/air/two/lookup.csv",
    )

    return merge(
        left=df,
        right=df_lookup,
        how="left",
        on=["NCFormat", "IPCC"],
    )


def agg_CO2e_by_category_and_year(df: DataFrame) -> DataFrame:
    """Aggregate "CO2 Equiv" by "OIF_category" and "EmissionYear".

    Args:
        df (DataFrame): A DataFrame to aggregate.

    Returns:
        DataFrame: An aggregated DataFrame.
    """
    return df.groupby(["OIF_category", "EmissionYear"]).sum("CO2 Equiv").reset_index()


def enrich_air_two(df: DataFrame) -> DataFrame:
    """Enriches the transformed air two DataFrame.

    This function maps "NCFormat" and "IPCC" values to OIF categories using a lookup
    table, then aggregates CO2e emissions by OIF category and year.

    Args:
        df (DataFrame): The transformed air two DataFrame.

    Returns:
        DataFrame: An enriched air two DataFrame.
    """
    return df.pipe(join_to_lookup).pipe(agg_CO2e_by_category_and_year)
