"""Tests for extract module."""
# Standard library imports
from contextlib import contextmanager
from json import dump
from typing import Any, Dict

# Third party imports
import pytest
from _pytest._code.code import ExceptionInfo
from _pytest.tmpdir import TempdirFactory
from pandas import DataFrame
from pandas.testing import assert_frame_equal

# Local imports
from oiflib.extract import (
    _column_name_to_string,
    _df_from_kwargs,
    _dict_from_json_local,
    _kwargs_from_dict,
    extract,
)


@pytest.fixture(scope="module")
def file_xlsx(tmpdir_factory: TempdirFactory, df_input: DataFrame) -> str:
    """Writes a DataFrame to a temporary Excel file, returns the path as a string.

    Args:
        tmpdir_factory (TempdirFactory): A pytest pytest.fixture for creating temporary
            directories.
        df_input (DataFrame): The DataFrame to write to a temporary file.

    Returns:
        str: The path of the temporary Excel file.
    """
    path = tmpdir_factory.mktemp("test").join("test.xlsx")

    path_as_string: str = str(path)

    df_input.to_excel(
        excel_writer=path_as_string,
        sheet_name="Sheet",
        startrow=5,
        startcol=5,
        index=False,
        float_format="%.2f",
    )

    return path_as_string


@pytest.fixture(scope="module")
def kwargs_input(file_xlsx: str) -> Dict[str, Any]:
    """Returns a dictionary of key word arguments to be passed to pandas.read_excel().

    Args:
        file_xlsx (str): The path of the temporary Excel file.

    Returns:
        Dict[str, Any]: A dictionary of key word arguments to be passed to
            pandas.read_excel().
    """
    return {
        "io": file_xlsx,
        "sheet_name": "Sheet",
        "usecols": [5, 6, 7],
        "skiprows": 5,
        "nrows": 2,
    }


@pytest.fixture(scope="module")
def dictionary_input(
    kwargs_input: Dict[str, Any]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Returns a minimal data dictionary for testing the extract module.

    Args:
        kwargs_input (Dict[str, Any]): A dictionary of key word
            arguments to be passed to pandas.read_excel().

    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: A minimal data dictionary for testing
            the extract module.
    """
    return {
        "theme": {
            "indicator": kwargs_input,
        },
    }


@pytest.fixture(scope="module")
def file_json(
    tmpdir_factory: TempdirFactory,
    dictionary_input: Dict[str, Dict[str, Dict[str, Any]]],
) -> str:
    """Converts dict to JSON object, writes to temp file, returns path as string.

    Args:
        tmpdir_factory (TempdirFactory): A pytest pytest.fixture for creating temporary
            directories.
        dictionary_input (Dict[str, Dict[str, Dict[str, Any]]]): The python dictionary
            to be converted to a JSON object and written to the temporary JSON file.

    Returns:
        str: The path of the temporary JSON file.
    """
    path = tmpdir_factory.mktemp("test").join("test.json")

    path_as_string: str = str(path)

    with open(path_as_string, "w") as file:
        dump(dictionary_input, file)

    return path_as_string


# Example-based tests
def test__dict_from_json_local(
    file_json: str,
    dictionary_input: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    """Returns a data dictionary from a JSON file.

    Args:
        file_json (str): The path of the temporary JSON file.
        dictionary_input (Dict[str, Dict[str, Dict[str, Any]]]): A minimal data
            dictionary for testing the extract module.
    """
    dictionary_output = _dict_from_json_local(file_path=file_json)

    assert dictionary_output == dictionary_input


@contextmanager
def does_not_raise():
    """Dummy doc string."""
    # TODO copied from https://docs.pytest.org/en/stable/example/parametrize.html#parametrizing-conditional-raising, not actually sure what it's doing  # noqa: B950 - URL
    yield


@pytest.mark.parametrize(
    "theme,indicator,expectation",
    [
        ("theme", "indicator", does_not_raise()),
        ("no theme", "indicator", pytest.raises(KeyError)),
        ("theme", "no indicator", pytest.raises(KeyError)),
    ],
)
def test__kwargs_from_dict(
    dictionary_input: Dict[str, Dict[str, Dict[str, Any]]],
    theme: str,
    indicator: str,
    kwargs_input: Dict[str, Any],
    expectation: ExceptionInfo,
) -> None:
    """Returns a kwargs dictionary from a data dictionary.

    Args:
        dictionary_input (Dict[str, Dict[str, Dict[str, Any]]]): A minimal data
            dictionary for testing the extract module.
        theme (str): Theme to look up in dictionary.
        indicator (str): Indicator to look up in dictionary.
        kwargs_input (Dict[str, Any]): A dictionary of key word arguments to be passed
            to pandas.read_excel().
        expectation (ExceptionInfo): Expection raised.
    """
    if dictionary_input:
        with expectation:
            kwargs_output: Dict[str, Any] = _kwargs_from_dict(
                dictionary=dictionary_input,
                theme=theme,
                indicator=indicator,
            )

            assert kwargs_output == kwargs_input
    else:
        kwargs_output = _kwargs_from_dict(
            dictionary=dictionary_input,
            theme=theme,
            indicator=indicator,
        )

        assert kwargs_output is None


def test__df_from_kwargs(
    kwargs_input: Dict[str, Any],
    df_input: DataFrame,
) -> None:
    """Passing the kwargs to pandas.read_excel() returns a DataFrame.

    Args:
        kwargs_input (Dict[str, Any]): A dictionary of key word arguments to be passed
            to pandas.read_excel().
        df_input (DataFrame): A minimal input DataFrame for testing the extract module.
    """
    df_output: DataFrame = _df_from_kwargs(
        kwargs_input,
    )

    assert_frame_equal(
        left=df_output,
        right=df_input,
    )


def test__column_name_to_string(df_input: DataFrame, df_output: DataFrame) -> None:
    """Column names have been converted to string.

    Args:
        df_input (DataFrame): A DataFrame with int column names.
        df_output (DataFrame): A DataFrame with those int column names converted to
            strings.
    """
    assert_frame_equal(
        left=df_input.pipe(_column_name_to_string),
        right=df_output,
    )


def test_extract(file_json: str, df_output: DataFrame) -> None:
    """Returns Dataframe with string column names from metadata in JSON file.

    Args:
        file_json (str): The path of the temporary JSON file.
        df_output (DataFrame): A minimal output DataFrame for testing the extract
            module.
    """
    df_output_actual: DataFrame = extract(
        theme="theme",
        indicator="indicator",
        file_path=file_json,
    )

    assert_frame_equal(
        left=df_output_actual,
        right=df_output,
    )
