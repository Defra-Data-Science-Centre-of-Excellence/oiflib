"""Tests for extract module."""
# Standard library imports
from contextlib import contextmanager
from json import dumps
from typing import Any, Dict, Optional

# Third party imports
import pytest
from _pytest._code.code import ExceptionInfo
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest_cases import fixture_ref, parametrize

# Local imports
from oiflib.extract import (
    _column_name_to_string,
    _df_from_kwargs,
    _dict_from_json,
    _dict_from_json_local,
    _dict_from_json_s3,
    _kwargs_from_dict,
    extract,
)


@contextmanager
def does_not_raise():
    """Dummy doc string."""
    # TODO copied from https://docs.pytest.org/en/stable/example/parametrize.html#parametrizing-conditional-raising, not actually sure what it's doing  # noqa: B950 - URL
    yield


# Example-based tests
def test__dict_from_json_local(
    file_json: str,
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    """Returns a data dictionary from a JSON file."""
    dictionary_output = _dict_from_json_local(file_path=file_json)

    assert dictionary_output == dict_theme


def test__dict_from_json_s3(
    test_s3_resource,
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
    bucket_name: str = "s3-ranch-019",
    object_key: str = "metadata_dictionary.json",
) -> None:
    """Returns same dictionary."""
    test_s3_resource.create_bucket(Bucket=bucket_name)

    test_s3_object = test_s3_resource.Object(
        bucket_name=bucket_name,
        key=object_key,
    )

    test_s3_object.put(
        Body=str(dumps(dict_theme)),
    )

    returned = _dict_from_json_s3(
        bucket_name=bucket_name,
        object_key=object_key,
    )

    assert returned == dict_theme


class TestDictFromJson(object):
    """Tests for _dict_from_json."""

    def test_local(
        self,
        file_json: str,
        dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
        bucket_name: Optional[str] = None,
        object_key: Optional[str] = None,
    ) -> None:
        """Path but no bucket or key returns dictionary."""
        returned = _dict_from_json(
            bucket_name=bucket_name,
            object_key=object_key,
            file_path=file_json,
        )

        assert returned == dict_theme

    def test_s3(
        self,
        test_s3_resource,
        dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
        bucket_name: str = "s3-ranch-019",
        object_key: str = "metadata_dictionary.json",
        file_path: Optional[str] = None,
    ) -> None:
        """Bucket and key but no path returns dictionary."""
        test_s3_resource.create_bucket(Bucket=bucket_name)

        test_s3_object = test_s3_resource.Object(
            bucket_name=bucket_name,
            key=object_key,
        )

        test_s3_object.put(
            Body=str(dumps(dict_theme)),
        )

        returned = _dict_from_json(
            bucket_name=bucket_name,
            object_key=object_key,
            file_path=file_path,
        )

        assert returned == dict_theme


@parametrize(
    "theme,indicator,kwargs_file,expectation",
    [
        ("theme", "indicator_xlsx", fixture_ref("kwargs_xlsx"), does_not_raise()),
        (
            "no theme",
            "indicator_xlsx",
            fixture_ref("kwargs_xlsx"),
            pytest.raises(KeyError),
        ),
        ("theme", "no indicator", None, pytest.raises(KeyError)),
        ("theme", "indicator_csv", fixture_ref("kwargs_csv"), does_not_raise()),
        (
            "no theme",
            "indicator_csv",
            fixture_ref("kwargs_csv"),
            pytest.raises(KeyError),
        ),
        ("theme", "no indicator", None, pytest.raises(KeyError)),
    ],
)
def test__kwargs_from_dict(
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
    theme: str,
    indicator: str,
    kwargs_file: Optional[Dict[str, Any]],
    expectation: ExceptionInfo,
) -> None:
    """Returns a kwargs dictionary from a data dictionary."""
    kwargs_output: Dict[str, Any]

    if dict_theme:
        with expectation:
            kwargs_output = _kwargs_from_dict(
                dictionary=dict_theme,
                theme=theme,
                indicator=indicator,
            )

            assert kwargs_output == kwargs_file
    else:
        kwargs_output = _kwargs_from_dict(
            dictionary=dict_theme,
            theme=theme,
            indicator=indicator,
        )

        assert kwargs_output is None


@parametrize(
    "exception, kwargs_file",
    [
        (does_not_raise(), fixture_ref("kwargs_xlsx")),
        (does_not_raise(), fixture_ref("kwargs_csv")),
        (pytest.raises(NotImplementedError), {"key": "value"}),
    ],
)
def test__df_from_kwargs(
    exception: ExceptionInfo,
    kwargs_file: Dict[str, Any],
    df_extracted_input: DataFrame,
) -> None:
    """Passing the kwargs to read_excel() or read_csv() returns a DataFrame."""
    # ! I can't find a way of stopping read_csv() converting int headers to object
    # ! so I'm explicitly converting them back to int so the test will pass.
    with exception:
        returned: DataFrame = _df_from_kwargs(
            kwargs_file,
        ).rename(columns=int)

        assert_frame_equal(
            left=returned,
            right=df_extracted_input,
        )


def test__column_name_to_string(
    df_extracted_input: DataFrame, df_extracted_output: DataFrame
) -> None:
    """Column names have been converted to string."""
    assert_frame_equal(
        left=df_extracted_input.pipe(_column_name_to_string),
        right=df_extracted_output,
    )


@parametrize(indicator=("indicator_xlsx", "indicator_csv"))
def test_extract(
    indicator: str, file_json: str, df_extracted_output: DataFrame
) -> None:
    """Returns Dataframe with string column names from metadata in JSON file."""
    returned: DataFrame = extract(
        theme="theme",
        indicator=indicator,
        bucket_name=None,
        object_key=None,
        file_path=file_json,
    )

    assert_frame_equal(
        left=returned,
        right=df_extracted_output,
    )
