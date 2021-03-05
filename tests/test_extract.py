"""Tests for extract module."""
# Standard library imports
from contextlib import contextmanager
from typing import Any, Dict

# Third party imports
import pytest
from _pytest._code.code import ExceptionInfo
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


# Example-based tests
def test__dict_from_json_local(
    file_json: str,
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    """Returns a data dictionary from a JSON file.

    Args:
        file_json (str): The path of the temporary JSON file.
        dict_theme (Dict[str, Dict[str, Dict[str, Any]]]): A minimal data
            dictionary for testing the extract module.
    """
    dictionary_output = _dict_from_json_local(file_path=file_json)

    assert dictionary_output == dict_theme


@contextmanager
def does_not_raise():
    """Dummy doc string."""
    # TODO copied from https://docs.pytest.org/en/stable/example/parametrize.html#parametrizing-conditional-raising, not actually sure what it's doing  # noqa: B950 - URL
    yield


@pytest.mark.parametrize(
    "theme,indicator,expectation",
    [
        ("theme", "indicator_xlsx", does_not_raise()),
        ("no theme", "indicator_xlsx", pytest.raises(KeyError)),
        ("theme", "no indicator", pytest.raises(KeyError)),
        ("theme", "indicator_csv", does_not_raise()),
        ("no theme", "indicator_csv", pytest.raises(KeyError)),
        ("theme", "no indicator", pytest.raises(KeyError)),
    ],
)
def test__kwargs_from_dict(
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
    theme: str,
    indicator: str,
    kwargs_xlsx: Dict[str, Any],
    expectation: ExceptionInfo,
) -> None:
    """Returns a kwargs dictionary from a data dictionary.

    Args:
        dict_theme (Dict[str, Dict[str, Dict[str, Any]]]): A minimal data
            dictionary for testing the extract module.
        theme (str): Theme to look up in dictionary.
        indicator (str): Indicator to look up in dictionary.
        kwargs_xlsx (Dict[str, Any]): A dictionary of key word arguments to be passed
            to pandas.read_excel().
        expectation (ExceptionInfo): Expection raised.
    """
    if dict_theme:
        with expectation:
            kwargs_output: Dict[str, Any] = _kwargs_from_dict(
                dictionary=dict_theme,
                theme=theme,
                indicator=indicator,
            )

            assert kwargs_output == kwargs_xlsx
    else:
        kwargs_output = _kwargs_from_dict(
            dictionary=dict_theme,
            theme=theme,
            indicator=indicator,
        )

        assert kwargs_output is None


def test__df_from_kwargs(
    kwargs_xlsx: Dict[str, Any],
    df_extracted_input: DataFrame,
) -> None:
    """Passing the kwargs to pandas.read_excel() returns a DataFrame.

    Args:
        kwargs_xlsx (Dict[str, Any]): A dictionary of key word arguments to be passed
            to pandas.read_excel().
        df_extracted_input (DataFrame): A minimal input DataFrame for testing the
            extract module.
    """
    df_extracted_output: DataFrame = _df_from_kwargs(
        kwargs_xlsx,
    )

    assert_frame_equal(
        left=df_extracted_output,
        right=df_extracted_input,
    )


def test__column_name_to_string(
    df_extracted_input: DataFrame, df_extracted_output: DataFrame
) -> None:
    """Column names have been converted to string.

    Args:
        df_extracted_input (DataFrame): A DataFrame with int column names.
        df_extracted_output (DataFrame): A DataFrame with those int column names
            converted to strings.
    """
    assert_frame_equal(
        left=df_extracted_input.pipe(_column_name_to_string),
        right=df_extracted_output,
    )


def test_extract(file_json: str, df_extracted_output: DataFrame) -> None:
    """Returns Dataframe with string column names from metadata in JSON file.

    Args:
        file_json (str): The path of the temporary JSON file.
        df_extracted_output (DataFrame): A minimal output DataFrame for testing the
            extract module.
    """
    df_extracted_output_actual: DataFrame = extract(
        theme="theme",
        indicator="indicator",
        bucket_name=None,
        object_key=None,
        file_path=file_json,
    )

    assert_frame_equal(
        left=df_extracted_output_actual,
        right=df_extracted_output,
    )
