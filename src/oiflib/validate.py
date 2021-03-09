r"""Functions for validating OIF DataFrames.

The library uses the pandera_ package for schema-based data validation.

.. _pandera: https://pandera.readthedocs.io/

It provides a generic :func:`validate` function that validates a given DataFrame against
a schema for a given ``theme``, ``indicator``, and processing ``stage``

The schema contains information about a DataFrame's columns. It define the column names,
data-types, and allow you to check the values against various constraints.

For example, the schema of the extracted Air One DataFrame contains the following
information:

::

    {
        "air": {
                "one": {
                        "extracted": DataFrameSchema(
                            columns={
                                    "ShortPollName": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "B[a]p",
                                                "B[a]p Total",
                                                "CO",
                                                ...
                                                "SO2 Total",
                                                "VOC",
                                                "VOC Total",
                                            ],
                                        ),
                                    ),
                                    "NFRCode": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "1A1a",
                                                "1A1b",
                                                "1A1c",
                                                ...
                                                "5D2",
                                                "5E",
                                                "6A",
                                            ],
                                        ),
                                        nullable=True,
                                    ),
                                    "SourceName": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "Accidental fires - dwellings",
                                                "Accidental fires - other buildings",
                                                "Accidental fires - vehicles",
                                                ...
                                                "Yarding",
                                                "Zinc alloy and semis production",
                                                "Zinc oxide production",
                                            ],
                                        ),
                                        nullable=True,
                                    ),
                                    r"\d{4}": Column(
                                        pandas_dtype=float,
                                        nullable=True,
                                        regex=True
                                    ),
                            },
                            coerce=True,
                            strict=True,
                        ),
                ...
                }
        ...
        }
    ...
    }

- This schema checks whether the columns ``ShortPollName``, ``NFRCode``, ``SourceName``,
  and any number of column names consisting of four digits exist.
- It checks that the first three contain string values, while the others contain float
  values.
- It checks that the first three contain values from pre-defined lists.
- It checks that ``ShortPollName`` doesn't contain any null values.

Schemas such as this power the validation function. If the DataFrame passed to the
validation function conforms to the schema, it is returned, if not the validation
function raises an error. This allows you insert validation functions calls between
each processing stage.

To import :func:`validate` run:

>>> from oiflib.core import validate

To use it to validate the extracted Air One DataFrame run:

>>> extracted_validated = validate(
    theme="air",
    indicator="one",
    stage="extracted",
    df=extracted
)
"""
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

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`_dict_from_pickle`, which is, in turn, called within
        :func:`validate`.

    Example:
        >>> dict = _dict_from_pickle_local(
            file_path="/path/to/file",
        )

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
    """Read OIF metadata dictionary from pickle file in S3.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`_dict_from_pickle`, which is, in turn, called within
        :func:`validate`.

    Example:
        >>> dict = _dict_from_pickle_s3(
            bucket_name="bucket name",
            object_key="object key",
        )

    Args:
        bucket_name (str): The s3 object's bucket_name identifier
        object_key (str): The s3 object's key identifier.

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
    """Read OIF metadata dictionary from pickle file (s3 or local).

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`validate`.

    Examples:
        To read a dictionary from a pickle file stored locally, provide ``file_path``
        and set ``bucket_name`` and ``object_key`` to ``None``.

        >>> dict = _dict_from_pickle(
            bucket_name=None,
            object_key=None,
            file_path="/path/to/file",
        )

        To read a dictionary from a pickle file stored in an s3 bucket, provide
        ``bucket_name`` and ``object_key`` and set ``file_path`` to ``None``.

        >>> dict = _dict_from_pickle(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

    Args:
        bucket_name (str): The s3 object's bucket_name identifier
        object_key (str): The s3 object's key identifier.
        file_path (str): path to pickle file containing OIF datasets dictionary.

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

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`validate`.

    Example:
        >>> dict = _dict_from_pickle(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

        >>> schema = _schema_from_dict(
            dict=dict,
            theme="air",
            indicator="one",
            stage="extracted",
        )

    Args:
        dict (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): The dictionary of
            DataFrameSchema.
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".
        stage (str): Stage in pipeline, as lower case string. Must be one of
            "extracted", "transformed" or "enriched".

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

    Example:
        To extract a DataFrame:

        >>> air_one_extracted = extract(
            theme="air",
            indicator="one",
        )

        To validate it against the appropriate DataFrameSchema:

        >>> air_one_extracted_validated = validate(
            theme="air",
            indicator="one",
            stage="extracted",
            df=air_one_extracted,
        )

    Args:
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".
        stage (str): Stage in pipeline, as lower case string. Must be one of
            "extracted", "transformed" or "enriched".
        df (DataFrame): A DataFrame to be validated.
        bucket_name (str): The s3 object's bucket_name identifier. Defaults to
            "s3-ranch-019".
        object_key (str): The s3 object's key identifier. Defaults to
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
