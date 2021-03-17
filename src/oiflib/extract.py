"""A generic extract function.

The generic :func:`extract` function extracts a DataFrame for a given ``theme``
and ``indicator`` from an Excel or OpenDocument workbook or CSV file. It uses the
``theme`` and ``indicator`` values to look up the location of the workbook or CSV in
a JSON format metadata dictionary.

The dictionary contains key-value pairs of parameters and arguments that are passed to
pandas read_excel_ or read_csv_ method.

.. _read_excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html  # noqa: B950 - URL

.. _read_csv: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html  # noqa: B950 - URL

For example, the dictionary contains the following information for Air One::

    {
        "air": {
            "one": {
                "io": "http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",  # noqa: B950 - URL
                "sheet_name": "England API",
                "usecols": "B:AA",
                "skiprows": 13,
                "nrows": 1602
            },
    ...
        }
    }

Where:

- ``io`` is the path to the workbook you want to extract a DataFrame from. This can be a
  URL (as above) or a local file path.
- ``sheet_name`` is the name of the sheet containing the DataFrame you want to extract.
- ``usecols`` is the column range the DataFrame you want to extract.
- ``skiprows`` is the number of rows (starting at 0) to skip before the header row of
  the DataFrame you want to extract.
- ``nrows`` is total number of rows to read, excluding the header row.

The above is therefore the equivalent of the range ``'England API'!$B$14:$AA$1616`` in
Excel notation.

To import the extract function run:

>>> from oiflib.core import extract

To use it to extract the Air One DataFrame, pass it the relevant ``theme`` and
``indicator`` arguments as lower-case strings:

>>> extracted = extract(theme="air", indicator="one")
"""
from json import load, loads
from typing import Dict, KeysView, Optional, Union

from boto3 import resource
from pandas import DataFrame, read_csv, read_excel

from oiflib._helper import _check_s3_or_local


def _dict_from_json_local(
    file_path: str,
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from a local JSON file.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`_dict_from_json`, which is, in turn, called within
        :func:`extract`.

    Example:
        Read the metadata dictionary from a local file:

        >>> dict = _dict_from_json_local(
            file_path="/path/to/file",
        )

    Args:
        file_path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    with open(file=file_path, mode="r") as file_json:
        dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = load(file_json)
    return dictionary


def _dict_from_json_s3(
    bucket_name: Optional[str],
    object_key: Optional[str],
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from JSON file in S3.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`_dict_from_json`, which is, in turn, called within
        :func:`extract`.

    Example:
        Read the metadata dictionary from s3:

        >>> dict = _dict_from_json_s3(
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
    json_string: str = (
        s3_resource.Bucket(bucket_name)
        .Object(object_key)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = loads(json_string)
    return dictionary


def _dict_from_json(
    bucket_name: Optional[str],
    object_key: Optional[str],
    file_path: Optional[str],
) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF metadata dictionary from JSON file (s3 or local).

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`extract`.

    Examples:
        To read the metadata dictionary from a local file, provide ``file_path``
        and set ``bucket_name`` and ``object_key`` to ``None``.

        >>> dict = _dict_from_json(
            bucket_name=None,
            object_key=None,
            file_path="/path/to/file",
        )

        To read the metadata dictionary from s3, provide ``bucket_name`` and
        ``object_key`` and set ``file_path`` to ``None``.

        >>> dict = _dict_from_json(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

    Args:
        bucket_name (str): The s3 object's bucket_name identifier
        object_key (str): The s3 object's key identifier.
        file_path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    source: str = _check_s3_or_local(
        bucket_name=bucket_name, object_key=object_key, file_path=file_path
    )

    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]]

    if source == "s3":
        dictionary = _dict_from_json_s3(
            bucket_name=bucket_name,
            object_key=object_key,
        )
    else:
        dictionary = _dict_from_json_local(
            file_path=str(file_path),
        )

    return dictionary


def _kwargs_from_dict(
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]],
    theme: str,
    indicator: str,
) -> Dict[str, Union[str, int]]:
    """Given a dictionary, returns the kwargs for a given theme and indicator.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`extract`.

    Example:
        Read the metadata dictionary from s3:

        >>> dictionary = _dict_from_json(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

        Extract a dictionary of kwargs for a given theme and indicator:

        >>> dictionary_kwargs = _kwargs_from_dict(
            dictionary = dictionary,
            theme="air",
            indicator="one",
        )

    Args:
        dictionary (Dict[str, Dict[str, Dict[str, Union[str, int]]]]): A dictionary of
            OIF datasets.
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".

    Raises:
        KeyError: If the ``theme`` or ``indicator`` does not exist in the
            ``dictionary``.

    Returns:
        Dict[str, Union[str, int]]: A dictionary of kwargs for a given theme and
        indicator.
    """
    try:
        dictionary_theme: Dict[str, Dict[str, Union[str, int]]] = dictionary[theme]
    except KeyError:
        print(f"Theme: { theme } does not exist.")
        raise
    else:
        try:
            dictionary_kwargs: Dict[str, Union[str, int]] = dictionary_theme[indicator]
        except KeyError:
            print(
                f"Indicator: { indicator } does not exist in Theme: { theme }.",
            )
            raise
        else:
            return dictionary_kwargs


def _df_from_kwargs(
    dictionary_kwargs: Dict[str, Union[str, int]],
) -> DataFrame:
    """Given a dictionary of kwargs, returns a DataFrame.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`extract`.

    Example:
        Read the metadata dictionary from s3:

        >>> dictionary = _dict_from_json(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

        Extract a dictionary of kwargs for a given theme and indicator:

        >>> dictionary_kwargs = _kwargs_from_dict(
            dictionary = dictionary,
            theme="air",
            indicator="one",
        )

        Read a DataFrame using the dictionary of kwargs:

        >>> df = _df_from_kwargs(
            dictionary_kwargs=dictionary_kwargs,
        )

    Args:
        dictionary_kwargs (Dict[str, Union[str, int]]): A dictionary of kwargs for a
            given theme and indicator.

    Raises:
        NotImplementedError: If the file to extract the DataFrame from is not .xls*,
            .odf, or .csv.

    Returns:
        DataFrame: The DataFrame given the dictionary of kwargs.
    """
    dict_keys: KeysView[str] = dictionary_kwargs.keys()

    if "io" in dict_keys:
        return read_excel(**dictionary_kwargs)
    elif "filepath_or_buffer" in dict_keys:
        return read_csv(**dictionary_kwargs)
    else:
        raise NotImplementedError(
            "At the moment, extract() only supports reading .xls* and .odf files using"
            "read_excel() or .csv files using read_csv()."
        )


def _column_name_to_string(df: DataFrame) -> DataFrame:
    """Converts the column names of a DataFrame to string.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`extract`.

    Example:
        Read the metadata dictionary from s3:

        >>> dictionary = _dict_from_json(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )

        Extract a dictionary of kwargs for a given theme and indicator:

        >>> dictionary_kwargs = _kwargs_from_dict(
            dictionary = dictionary,
            theme="air",
            indicator="one",
        )

        Read a DataFrame using the dictionary of kwargs:

        >>> df = _df_from_kwargs(
            dictionary_kwargs=dictionary_kwargs,
        )

        Convert the DataFrame's column names to string:

        >>> df_string_column_names = _column_name_to_string(
            df=df,
        )

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
    file_path: Optional[str] = None,
) -> DataFrame:
    """Reads in data for the theme and indicator specified.

    This function extracts a DataFrame from an Excel or OpenDocument workbook or CSV
    file based on the metadata provided by the oif_datasets metadata dictionary. First,
    it searches for the given ``theme`` within oif_datasets. If it finds this, it
    searches for the ``indicator`` within theme metadata dictionary. If it finds this,
    it unpacks the keys and values and uses them as paramaters and arguments for pandas
    read_excel() or read_csv(). Finally, it converts the column names of the extracted
    DataFrame to string. This convertion is necessary for subsequent validation.

    Example:
        >>> air_one_extracted = extract(
            theme="air",
            indicator="one",
        )

    Under the hood, this function calls :func:`_dict_from_json`,
    :func:`_kwargs_from_dict`, :func:`_df_from_kwargs`, and
    :func:`_column_name_to_string`.

    Args:
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".
        bucket_name (str): The s3 object's bucket_name identifier. Defaults to
            "s3-ranch-019".
        object_key (str): The s3 object's key identifier. Defaults to
            "metadata_dictionary.json".
        file_path (Optional[str], optional): path to JSON file containing OIF datasets
            dictionary. Defaults to None.

    Returns:
        DataFrame: The DataFrame for the given theme and indicator.
    """
    dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = _dict_from_json(
        bucket_name=bucket_name,
        object_key=object_key,
        file_path=file_path,
    )

    dictionary_kwargs: Dict[str, Union[str, int]] = _kwargs_from_dict(
        dictionary=dictionary,
        theme=theme,
        indicator=indicator,
    )

    df: DataFrame = _df_from_kwargs(
        dictionary_kwargs=dictionary_kwargs,
    )

    df_string_column_names: DataFrame = _column_name_to_string(
        df=df,
    )

    return df_string_column_names
