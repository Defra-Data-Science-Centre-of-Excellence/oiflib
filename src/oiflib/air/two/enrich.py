"""Functions to enrich the air two data."""

from pandas import DataFrame, merge, read_csv


def _join_to_lookup(
    df: DataFrame,
    path_to_lookup: str,
) -> DataFrame:
    """Join to "NCFormat" + "IPCC" to "OIF_category" lookup table.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`enrich_air_two`.


    Example:
        >>> _joined: DataFrame = _join_to_lookup(
            df=df,
            path_to_lookup=path_to_lookup,
        )

    Args:
        df (DataFrame): An in-memory DataFrame.
        path_to_lookup (str): Path to lookup DataFrame.

    Returns:
        DataFrame: A DataFrame with "OIF_category" column.
    """
    _lookup: DataFrame = read_csv(
        filepath_or_buffer=path_to_lookup,
    )

    return merge(
        left=df,
        right=_lookup,
        how="left",
        on=["NCFormat", "IPCC"],
    )


def _agg_CO2e_by_category_and_year(df: DataFrame) -> DataFrame:
    """Aggregate "CO2 Equiv" by "OIF_category" and "EmissionYear".

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`enrich_air_two`.

    Example:
        >>> _aggregated: DataFrame = _agg_CO2e_by_category_and_year(
            df=_joined,
        )


    Args:
        df (DataFrame): A DataFrame to aggregate.

    Returns:
        DataFrame: An aggregated DataFrame.
    """
    return df.groupby(["OIF_category", "EmissionYear"]).sum("CO2 Equiv").reset_index()


def enrich_air_two(
    df: DataFrame,
    path_to_lookup: str = "s3://s3-ranch-019/got/air_two_lookup.csv",
) -> DataFrame:
    """Enriches the transformed air two DataFrame.

    Under the hood, this function calls :func:`_join_to_lookup` to map ``NCFormat`` and
    ``IPCC`` values to OIF categories and :func:`_agg_CO2e_by_category_and_year`
    to aggregate CO2e emissions by OIF category and year.

    Example:
        >>> air_two_enriched = enrich_air_two(air_two_transformed_validated)

    Args:
        df (DataFrame): The transformed air two DataFrame.
        path_to_lookup (str): Path to lookup DataFrame. Defaults to
            "s3://s3-ranch-019/got/air_two_lookup.csv".

    Returns:
        DataFrame: An enriched air two DataFrame.
    """
    _joined: DataFrame = _join_to_lookup(
        df=df,
        path_to_lookup=path_to_lookup,
    )

    _aggregated: DataFrame = _agg_CO2e_by_category_and_year(
        df=_joined,
    )

    return _aggregated
