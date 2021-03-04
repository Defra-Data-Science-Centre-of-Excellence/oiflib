"""Tests for the Air Three transform module."""
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.three.transform import (
    filter_rows,
    select_columns,
    split_variable_column,
    transform_air_three,
    unpivot,
)


def test_unpivot(extracted: DataFrame, extracted_unpivoted: DataFrame) -> None:
    """The PM2.5 column names and values are unpivoted."""
    expected: DataFrame = extracted_unpivoted

    output = unpivot(
        extracted,
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )


def test_split_variable_column(
    extracted_unpivoted: DataFrame,
    extracted_split: DataFrame,
) -> None:
    """The year and measure elements are split into seperate columns."""
    expected: DataFrame = extracted_split

    output = split_variable_column(
        extracted_unpivoted,
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )


def test_select_columns(
    extracted_split: DataFrame,
    extracted_selected: DataFrame,
) -> None:
    """The original "variable" column is dropped."""
    expected: DataFrame = extracted_selected

    output = select_columns(
        extracted_split,
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )


def test_filter_rows(extracted_selected: DataFrame, transformed: DataFrame) -> None:
    """The England Total rows are selected."""
    expected: DataFrame = transformed

    output = filter_rows(
        extracted_selected,
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )


def test_transform_air_three(extracted: DataFrame, transformed: DataFrame) -> None:
    """Data unpivoted, year & measure cols created, and Eng total rows selected."""
    expected: DataFrame = transformed

    output = transform_air_three(
        extracted,
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )
