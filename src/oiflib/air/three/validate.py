"""Functions to validate Air Three DataFrames."""

from typing import Union

from pandas import DataFrame
from pandera.errors import SchemaError

from oiflib.air.three.schemas import schema_extracted, schema_transformed


def validate_air_three_extracted(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the Air Three extracted schema.

    If the schema validation checks pass, the input DataFrame is returned.
    However, if they don't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_extracted(df)


def validate_air_three_transformed(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the Air Three transformed schema.

    If the schema validation checks pass, the input DataFrame is returned.
    However, if they don't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_transformed(df)
