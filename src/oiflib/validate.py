"""Functions for validating OIF DataFrames."""
from typing import Dict, Union

from pandas import DataFrame
from pandera import DataFrameSchema
from pandera.errors import SchemaError

from oiflib.schema import dict_schema


def _schema_from_dict(
    theme: str,
    indicator: str,
    stage: str,
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]] = dict_schema,
) -> DataFrameSchema:
    """Returns schema for given theme, indicator, and stage.

    Args:
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Stage in pipeline, as lower case string.
        dict (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): The dictionary of
            schema.

    Returns:
        DataFrameSchema: Schema for given theme, indicator, and stage.


    """
    return dict[theme][indicator][stage]


def validate(
    theme: str, indicator: str, stage: str, df: DataFrame
) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against a schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Stage in pipeline, as lower case string.
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    schema: DataFrameSchema = _schema_from_dict(
        theme=theme, indicator=indicator, stage=stage
    )

    return schema(df)
