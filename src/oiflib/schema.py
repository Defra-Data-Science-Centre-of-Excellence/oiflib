"""Schema for Air Five DataFrames."""
from json import dumps as dump_json_to_string
from json import load as load_json_from_file
from json import loads as load_json_from_string
from typing import Dict, Optional, Tuple, Union

from boto3 import resource
from dill import dump as dump_pickle_to_file  # noqa: S403 - security warnings n/a
from dill import dumps as dump_pickle_to_string  # noqa: S403 - security warnings n/a
from dill import load as load_pickle_from_file  # noqa: S403 - security warnings n/a
from dill import loads as load_pickle_from_string  # noqa: S403 - security warnings n/a
from pandera import DataFrameSchema


def get(
    source: Union[str, Tuple[str, str]],
) -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """Read an OIF schema or metadata dictionary from s3 or local file.

    Examples:
        To read the schema dictionary from a pickle file stored in the s3 bucket:

        >>> oif_schema_dictionary = get(
            source=("s3-ranch-019", "schemas.pkl")
        )

        To read the schema dictionary from a pickle file stored in a local file:

        >>> oif_schema_dictionary = get(
            source="/path/to/local/schemas.pkl"
        )

        To read the metadata dictionary from a json file stored in the s3 bucket:

        >>> oif_metadata_dictionary = get(
            source=("s3-ranch-019", "metadata_dictionary.json")
        )

        To read the metadata dictionary from a json file stored in a local file:

        >>> oif_metadata_dictionary = get(
            source="/path/to/local/metadata_dictionary.json"
        )

    Args:
        source (Union[str, Tuple[str, str]]): The s3 bucket name and object key as a
            tuple or local file path as a string.

    Raises:
        NotImplementedError: If the source file extension is not .pkl or .json

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: The schema of metadata
            dictionary you want to update.
    """
    if isinstance(source, tuple):
        s3_resource = resource("s3")

        s3_object: bytes = (
            s3_resource.Object(
                bucket_name=source[0],
                key=source[1],
            )
            .get()["Body"]
            .read()
        )

        if source[1].endswith(".pkl"):
            return load_pickle_from_string(s3_object)  # noqa: S301: Security n/a

        elif source[1].endswith(".json"):
            return load_json_from_string(s3_object)  # noqa: S301: Security n/a

        else:
            raise NotImplementedError("Only implemented for .pkl and .json")

    elif isinstance(source, str):
        with open(source, "rb") as file:
            if source.endswith(".pkl"):
                return load_pickle_from_file(file)  # noqa: S301: Security n/a

            elif source.endswith(".json"):
                return load_json_from_file(file)  # noqa: S301: Security n/a

            else:
                raise NotImplementedError("Only implemented for .pkl and .json")

    else:
        raise NotImplementedError("Only implemented for S3 and file paths")


def update(
    dictionary_one: Dict[
        Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]
    ],
    dictionary_two: Union[
        Dict[Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]],
        Dict[Optional[str], Dict[Optional[str], DataFrameSchema]],
        Dict[Optional[str], DataFrameSchema],
    ],
    theme: Optional[str] = None,
    indicator: Optional[str] = None,
    stage: Optional[str] = None,
) -> Dict[Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]]:
    """Updates an OIF schema or metadata dictionary.  # noqa: D412, B950 - flagged in error?, URL.

    Examples:

        Schema

        To update an indicator within an existing theme or add a new indicator to an
        existing theme:

        Read the existing schema dictionary into memory:

        >>> old_oif_schema_dictionary = get(
            source=("s3-ranch-019", "schemas.pkl")
        )

        Define a new dictionary for the schema you want to add or update:

        >>> new_indicator_dictionary = {
            {"three":
                "formatted": DataFrameSchema(
                    ...
                ),
            },
        }

        Specify the name of the theme within the update function:

        >>> new_oif_schema_dictionary = update(
            dictionary_one=old_oif_schema_dictionary,
            dictionary_two=new_indicator_dictionary,
            theme="air",
        )

        To update a stage within an existing indicator or add a new stage to an
        existing indicator:

        Read the existing schema dictionary into memory:

        >>> old_oif_schema_dictionary = get(
            source=("s3-ranch-019", "schemas.pkl")
        )

        Define a new dictionary for the schema you want to add or update:

        >>> new_stage_dictionary = {
                "formatted": DataFrameSchema(
                    ...
                ),
            }

        Specify the name of the theme and indicator within the update function:

        >>> new_oif_schema_dictionary = update(
            dictionary_one=old_oif_schema_dictionary,
            dictionary_two=new_indicator_dictionary,
            theme="air",
            indicator="six",
        )

        Metadata

        To update an indicator within an existing theme or add a new indicator to an
        existing theme:

        Read the existing metadata dictionary into memory:

        >>> old_oif_metadata_dictionary = get(
            source=("s3-ranch-019", "metadata_dictionary.json")
        )

        Define a new dictionary for the indicator you want to add or update:

        >>> new_indicator_dictionary = {
            'io': 'http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx',
            'sheet_name': 'England API',
            'usecols': 'B:AA',
            'skiprows': 13,
            'nrows': 1602
        }

        Specify the name of the theme and indicator within the update function:

        >>> new_oif_metadata_dictionary = update(
            dictionary_one=old_oif_metadata_dictionary,
            dictionary_two=new_indicator_dictionary,
            theme="air",
            indicator="one",
        )

    Args:
        dictionary_one (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): The schema
            of metadata dictionary you want to update.
        dictionary_two (Union[ Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
            Dict[str, Dict[str, DataFrameSchema]], Dict[str, DataFrameSchema], ]): A
            dictionary of theme, indicator, or stage information that you want to add
            or amend.
        theme (Optional[str]): Theme name, as a lower case string. E.g. "air". Defaults
            to None.
        indicator (Optional[str]): Indicator number, as a lower case string. E.g. "one".
            Defaults to None.
        stage (Optional[str], optional): Stage name, as a lower case string. E.g.
            "extracted". Defaults to None.

    Raises:
        ValueError: If an invalid combination of theme, indicator, and/or stage is
            supplied.

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: An updated schema or metadata
            dictionary.
    """
    if not any([theme, indicator, stage]):
        dictionary_one.update(dictionary_two)
    elif theme and not any([stage, indicator]):
        dictionary_one[theme].update(dictionary_two)
    elif all([theme, indicator]) and not stage:
        dictionary_one[theme][indicator].update(dictionary_two)
    elif all([theme, indicator, stage]):
        dictionary_one[theme][indicator][stage].update(dictionary_two)
    else:
        raise ValueError("Invalid combination of theme, indicator, and/or stage")

    return dictionary_one


def put(
    dictionary: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    destination: Union[str, Tuple[str, str]],
) -> None:
    """Write an updated OIF schema or metadata dictionary back to s3 or local file.

    Examples:
        To write an updated schema dictionary to the s3 bucket:

        >>> put(
            dictionary=new_oif_schema_dictionary,
            destination=("s3-ranch-019", "schemas.pkl")
        )

        To write an updated schema dictionary to a local file:

        >>> put(
            dictionary=new_oif_schema_dictionary,
            destination="/path/to/local/schemas.pkl"
        )

        To write an updated metadata dictionary to the s3 bucket:

        >>> put(
            dictionary=new_oif_schema_dictionary,
            destination=("s3-ranch-019", "metadata_dictionary.json")

        To write an updated metadata dictionary to a local file:

        >>> put(
            dictionary=new_oif_schema_dictionary,
            destination="/path/to/local/metadata_dictionary.json"
        )

    Args:
        dictionary (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): An updated
            schema or metadata dictionary.
        destination (Union[str, Tuple[str, str]]): The s3 bucket name and object key as
            a tuple or local file path as a string.

    Raises:
        NotImplementedError: If the destination file extension is not .pkl or .json
    """
    if isinstance(destination, tuple):
        s3_resource = resource("s3")

        object_body: Union[bytes, str]

        if destination[1].endswith(".pkl"):
            object_body = dump_pickle_to_string(
                obj=dictionary,
            )

        elif destination[1].endswith(".json"):
            object_body = dump_json_to_string(
                obj=dictionary,
            )

        else:
            raise NotImplementedError("Only implemented for .pkl and .json")

        s3_resource.Object(bucket_name=destination[0], key=destination[1],).put(
            ACL="bucket-owner-full-control",
            Body=object_body,
        )

    elif isinstance(destination, str):

        if destination.endswith(".pkl"):
            with open(destination, "wb") as file:
                dump_pickle_to_file(
                    obj=dictionary,
                    file=file,
                )

        elif destination.endswith(".json"):
            with open(destination, "wb") as file:
                dump_json_to_string(
                    obj=dictionary,
                    fp=file,
                )

        else:
            raise NotImplementedError("Only implemented for .pkl and .json")

    else:
        raise NotImplementedError("Only implemented for S3 and file paths")
