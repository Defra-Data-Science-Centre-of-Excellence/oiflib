"""Functions for validating OIF DataFrames."""
from tempfile import NamedTemporaryFile
from typing import Dict, Optional, Union

from boto3 import resource
from dill import load  # noqa: S403 - security warnings n/a
from pandas import DataFrame
from pandera import DataFrameSchema
from pandera.errors import SchemaError

from oiflib._helper import _check_s3_or_local


def _dict_from_pickle_local(
    file_path: str,
) -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """Returns dictionary of DataFrameSchema from file path.

    Args:
        file_path (str): Path to file containing dictionary of DataFrameSchema.

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: Dictionary of
            DataFrameSchema.
    """
    with open(file=file_path, mode="rb") as file:
        dictionary: Dict[
            str, Dict[str, Dict[str, DataFrameSchema]]
        ] = load(  # noqa: S301 - security warnings n/a
            file
        )
    return dictionary


def _dict_from_pickle_s3(
    bucket_name: Optional[str],
    object_key: Optional[str],
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from JSON file in S3.

    Args:
        bucket_name (str): # TODO [description]
        object_key (str): # TODO [description]

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    s3_resource = resource("s3")

    tmp = NamedTemporaryFile(suffix=".pkl").name

    with open(tmp, "wb") as file:
        s3_resource.Bucket(bucket_name).download_fileobj(object_key, file)

    with open(tmp, "rb") as file:
        dictionary: Dict[
            str,
            Dict[str, Dict[str, Union[str, int]]],
        ] = load(  # noqa: S301 - security warnings n/a
            file
        )

    # ? Do I need to remove the NamedTemporaryFile?

    return dictionary


def _dict_from_pickle(
    bucket_name: Optional[str],
    object_key: Optional[str],
    file_path: Optional[str],
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from JSON file (s3 or local).

    Args:
        bucket_name (str): # TODO [description].
        object_key (str): # TODO [description].
        file_path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    source: str = _check_s3_or_local(
        bucket_name=bucket_name, object_key=object_key, file_path=file_path
    )

    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]]

    if source == "s3":
        dictionary = _dict_from_pickle_s3(
            bucket_name=bucket_name,
            object_key=object_key,
        )
    else:
        dictionary = _dict_from_pickle_local(
            file_path=str(file_path),
        )

    return dictionary


def _schema_from_dict(
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    theme: str,
    indicator: str,
    stage: str,
) -> DataFrameSchema:
    """Returns DataFrameSchema from dictionary of DataFrameSchema.

    Args:
        dict (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): The dictionary of
            DataFrameSchema.
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Processing stage, as lower case string.

    Returns:
        DataFrameSchema: DataFrameSchema for given theme, indicator, and stage.


    """
    return dict[theme][indicator][stage]


def validate(
    theme: str,
    indicator: str,
    stage: str,
    df: DataFrame,
    bucket_name: Optional[str] = "s3-ranch-019",
    object_key: Optional[str] = "schemas.pkl",
    file_path: Optional[str] = None,
) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against a DataFrameSchema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Stage in pipeline, as lower case string.
        df (DataFrame): A DataFrame to be validated.
        bucket_name (Optional[str], optional): [description]. Defaults to
            "s3-ranch-019".
        object_key (Optional[str], optional): [description]. Defaults to
            "schemas.pkl".
        file_path (Optional[str], optional): path to dill pickled file containing
            schemas. Defaults to None.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]] = _dict_from_pickle(
        bucket_name=bucket_name,
        object_key=object_key,
        file_path=file_path,
    )

    schema: DataFrameSchema = _schema_from_dict(
        dict=dict,
        theme=theme,
        indicator=indicator,
        stage=stage,
    )

    return schema(df)
