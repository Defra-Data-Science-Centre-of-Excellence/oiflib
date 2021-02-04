"""Validation functions of Air Four DataFrames."""

# TODO Repeating myself, these functions need to be abstracted to a higher level

from typing import Union

from pandas import DataFrame
from pandera.errors import SchemaError

from oiflib.air.four.schemas import schema_enriched, schema_extracted


def validate_air_four_extracted(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_four_extracted schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_extracted(df)


def validate_air_four_enriched(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_four_enriched schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_enriched(df)
