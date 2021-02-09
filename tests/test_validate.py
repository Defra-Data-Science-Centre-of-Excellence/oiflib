"""Tests for the validate module."""

from typing import Dict
from unittest.mock import patch

from numpy import float64, int64
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pandera import Check, Column, DataFrameSchema
from pytest import fixture

from oiflib.validate import _schema_from_dict, validate


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


def test__dict_from_path() -> None:
    """Placeholder."""
    assert True


def test__schema_from_dict(schema_dict, schema) -> None:
    """The expected schema is returned from the schema dictionary."""
    _ = _schema_from_dict(
        theme="test_theme",
        indicator="test_indicator",
        stage="test_stage",
        dict=schema_dict,
    )

    assert _ == schema


@patch("oiflib.validate._dict_from_path")
def test_validate(
    mock__dict_from_path: DataFrameSchema,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    df_output: DataFrame,
) -> None:
    """Validating a valid DataFrame returns that DataFrame."""
    mock__dict_from_path.return_value = schema_dict

    _ = validate(
        theme="test_theme", indicator="test_indicator", stage="test_stage", df=df_output
    )

    assert_frame_equal(
        left=_,
        right=df_output,
    )
