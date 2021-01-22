"""TODO module docstring."""

from pandas import DataFrame

from oiflib.air.one.schemas import schema_enriched, schema_extracted, schema_transformed


def validate_air_one_extracted(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return schema_extracted(df)


def validate_air_one_transformed(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return schema_transformed(df)


def validate_air_one_enriched(df: DataFrame) -> DataFrame:
    """TODO function docstring.

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return schema_enriched(df)
