"""Tests for the validate module."""
from typing import Dict

from _pytest.tmpdir import TempdirFactory
from dill import dump  # noqa: S403 - security warnings n/a
from numpy import float64, int64
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pandera import Check, Column, DataFrameSchema
from pytest import fixture

from oiflib.validate import _dict_from_pickle_local, _schema_from_dict, validate


@fixture(scope="module")
def schema_dict() -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """A minimal schema dictionary for testing the validate module."""
    return {
        "test_theme": {
            "test_indicator": {
                "test_stage": DataFrameSchema(
                    columns={
                        "1": Column(
                            pandas_dtype=str,
                            checks=Check.isin(["a", "b"]),
                            allow_duplicates=False,
                        ),
                        "2": Column(
                            pandas_dtype=int64,
                            checks=Check.in_range(4, 5),
                            allow_duplicates=False,
                        ),
                        "3": Column(
                            pandas_dtype=float64,
                            checks=Check.in_range(6.0, 9.0),
                            allow_duplicates=False,
                        ),
                    },
                    coerce=True,
                    strict=True,
                ),
            },
        },
    }


@fixture(scope="module")
def file_pkl(
    tmpdir_factory: TempdirFactory,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
) -> str:
    """Dill pickles dict, writes to temp file, returns path as string.

    Args:
        tmpdir_factory (TempdirFactory): A pytest pytest.fixture for creating temporary
            directories.
        schema_dict (Dict[str, Dict[str, Dict[str, Any]]]): The dictionary of
            DataFrameSchema to be dill pickled and written to the temporary file.

    Returns:
        str: The path of the temporary JSON file.
    """
    path = tmpdir_factory.mktemp("test").join("test.pkl")

    path_as_string: str = str(path)

    with open(path_as_string, "wb") as file:
        dump(schema_dict, file)

    return path_as_string


@fixture(scope="module")
def schema() -> DataFrameSchema:
    """A minimal schema for testing the validate module."""
    return DataFrameSchema(
        columns={
            "1": Column(
                pandas_dtype=str,
                checks=Check.isin(["a", "b"]),
                allow_duplicates=False,
            ),
            "2": Column(
                pandas_dtype=int64,
                checks=Check.in_range(4, 5),
                allow_duplicates=False,
            ),
            "3": Column(
                pandas_dtype=float64,
                checks=Check.in_range(6.0, 9.0),
                allow_duplicates=False,
            ),
        },
        coerce=True,
        strict=True,
    )


def test__dict_from_pickle_local(
    file_pkl: str,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
) -> None:
    """Placeholder."""
    _ = _dict_from_pickle_local(
        file_path=file_pkl,
    )
    assert _ == schema_dict


def test__schema_from_dict(schema_dict, schema) -> None:
    """The expected schema is returned from the schema dictionary."""
    _ = _schema_from_dict(
        theme="test_theme",
        indicator="test_indicator",
        stage="test_stage",
        dict=schema_dict,
    )

    assert _ == schema


def test_validate(
    file_pkl: str,
    df_output: DataFrame,
) -> None:
    """Validating a valid DataFrame returns that DataFrame."""
    _ = validate(
        theme="test_theme",
        indicator="test_indicator",
        stage="test_stage",
        df=df_output,
        bucket_name=None,
        object_key=None,
        file_path=file_pkl,
    )

    assert_frame_equal(
        left=_,
        right=df_output,
    )
