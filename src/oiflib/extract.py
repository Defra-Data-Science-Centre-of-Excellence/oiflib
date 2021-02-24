"""A generic extract function.

# TODO refactor the following as module docstring

The generic :ref:`extract_function` function extracts a DataFrame for a given **theme**
and **indicator** from an Excel or OpenDocument workbook. It uses the **theme** and
**indicator** values to look up the location of the workbook in a JSON format metadata
dictionary_.

.. _dictionary: https://github.com/Defra-Data-Science-Centre-of-Excellence/OIF-Pipeline-Logic/blob/EFT-Defra/issue33/data/datasets.json  # noqa: B950

The dictionary contains key-value pairs of parameters and arguments that are passed to
pandas.read_excel_ method.

.. _pandas.read_excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

For example, the dictionary contains the following information for Air One::

    {
        "air": {
            "one": {
                "io": "http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",
                "sheet_name": "England API",
                "usecols": "B:AA",
                "skiprows": 13,
                "nrows": 1602
            },
    ...
        }
    }

Where:

- **io** is the path to the workbook you want to extract a DataFrame from. This can be a
  URL (as above) or a local file path.
- **sheet_name** is the name of the sheet containing the DataFrame you want to extract.
- **usecols** is the column range the DataFrame you want to extract.
- **skiprows** is the number of rows (starting at 0) to skip before the header row of the
  DataFrame you want to extract.
- **nrows** is total number of rows to read, excluding the header row.

The above is therefore the equivalent of the range ``'England API'!$B$14:$AA$1616`` in Excel
notation.

To import the extract function run:

>>> from oiflib.core import extract

To use it to extract the Air One DataFrame, pass it the relevant **theme** and
**indicator** arguments as lower-case strings:

>>> extracted = extract(theme="air", indicator="one")
"""

from json import load, loads
from typing import Dict, Optional, Union

from boto3 import resource
from pandas import DataFrame, read_excel


def _dict_from_json_local(
    path: str,
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from a local JSON file.

    Args:
        path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    with open(file=path, mode="r") as file_json:
        dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = load(file_json)
    return dictionary


def _dict_from_json_s3(
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
    json_string: str = (
        s3_resource.Object(bucket_name=bucket_name, object_key=object_key)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = loads(json_string)
    return dictionary


def _dict_from_json_kwarg_check(
    bucket_name: Optional[str],
    object_key: Optional[str],
    path: Optional[str],
) -> str:
    """# TODO [summary].

    Args:
        bucket_name (str): # TODO [description].
        object_key (str): # TODO [description].
        path (str): # TODO [description].

    Raises:
        ValueError: # TODO [description]

    Returns:
        str: # TODO [description]
    """
    if bucket_name and object_key and not path:
        return "s3"
    elif not bucket_name and not object_key and path:
        return "local"
    else:
        raise ValueError(
            "You must supply either bucket_name and object_key to read from s3 or path \
            to read from a local file"
        )


def _dict_from_json(
    bucket_name: Optional[str],
    object_key: Optional[str],
    path: Optional[str],
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from JSON file (s3 or local).

    Args:
        bucket_name (str): # TODO [description].
        object_key (str): # TODO [description].
        path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    source: str = _dict_from_json_kwarg_check(
        bucket_name=bucket_name, object_key=object_key, path=path
    )

    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]]

    if source == "s3":
        dictionary = _dict_from_json_s3(
            bucket_name=bucket_name,
            object_key=object_key,
        )
    else:
        dictionary = _dict_from_json_local(
            path=str(path),
        )

    return dictionary


def _kwargs_from_dict(
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]],
    theme: str,
    indicator: str,
) -> Dict[str, Union[str, int]]:
    try:
        dictionary_theme: Dict[str, Dict[str, Union[str, int]]] = dictionary[theme]
    except KeyError:
        print(f"Theme: { theme } does not exist.")
        raise
    else:
        try:
            dictionary_indicator: Dict[str, Union[str, int]] = dictionary_theme[
                indicator
            ]
        except KeyError:
            print(
                f"Indicator: { indicator } does not exist in Theme: { theme }.",
            )
            raise
        else:
            return dictionary_indicator


def _df_from_kwargs(
    dictionary_indicator: Dict[str, Union[str, int]],
) -> DataFrame:
    return read_excel(**dictionary_indicator)


def _column_name_to_string(df: DataFrame) -> DataFrame:
    """Converts the column names of a DataFrame to string.

    Args:
        df (DataFrame): A DataFrame with column names to convert.

    Returns:
        DataFrame: A DataFrame with column names converted to string.
    """
    df.columns = df.columns.astype(str)
    return df


def extract(
    theme: str,
    indicator: str,
    bucket_name: Optional[str] = "s3-ranch-019",
    object_key: Optional[str] = "metadata_dictionary.json",
    path: Optional[str] = None,
) -> DataFrame:
    """Reads in data for the theme and indicator specified.

    This function extracts a DataFrame from an Excel or OpenDocument workbook based on
    the metadata provided by the oif_datasets metadata dictionary. First, it searches
    for the given "theme" within oif_datasets. If it finds this, it searches for the
    "indicator" within theme metadata dictionary. If it finds this, it unpacks the keys
    and values and uses them as paramaters and arguments for read_excel(). Finally, it
    converts the column names of the extracted DataFrame to string using
    column_name_to_string(). This convertion is necessary for subsequent validation.

    Args:
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".
        bucket_name (Optional[str], optional): [description]. Defaults to
            "s3-ranch-019".
        object_key (Optional[str], optional): [description]. Defaults to
            "metadata_dictionary.json".
        path (Optional[str], optional): path to JSON file containing OIF datasets
            dictionary. Defaults to None.

    Returns:
        DataFrame: The DataFrame for the given theme and indicator.
    """
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = _dict_from_json(
        bucket_name=bucket_name, object_key=object_key, path=path
    )

    kwargs: Dict[str, Union[str, int]] = _kwargs_from_dict(
        dictionary,
        theme,
        indicator,
    )

    df: DataFrame = _df_from_kwargs(kwargs)

    return df.pipe(_column_name_to_string)
