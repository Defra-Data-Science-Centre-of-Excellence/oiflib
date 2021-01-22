"""Tests for oiflib core functions."""

from typing import List

from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal

from oiflib.core import column_name_to_string, index_to_base_year


def test_column_name_to_string():
    """Column names have been converted to string."""
    col1_vals: List = ["a", "b", "c"]
    col2_vals: List = ["d", "e", "f"]
    col3_vals: List = ["g", "h", "i"]

    test_input: DataFrame = DataFrame(
        {
            1: col1_vals,
            2: col2_vals,
            3: col3_vals,
        },
    )

    test_expected: DataFrame = DataFrame(
        {
            "1": col1_vals,
            "2": col2_vals,
            "3": col3_vals,
        },
    )

    assert_frame_equal(
        left=test_input.pipe(column_name_to_string),
        right=test_expected,
    ) and assert_frame_equal(
        left=test_input.pipe(column_name_to_string),
        right=test_input,
    ) is False


def test_index_to_base_year():
    """Each item has been divided by the first item, then multipied by 100."""
    test_input: Series = Series(
        [2.0, 3.0, 4.0, 5.0, 6.0],
    )

    test_expected: Series = Series(
        [
            test_input[0] / test_input[0] * 100,
            test_input[1] / test_input[0] * 100,
            test_input[2] / test_input[0] * 100,
            test_input[3] / test_input[0] * 100,
            test_input[4] / test_input[0] * 100,
        ],
    )

    assert_series_equal(
        left=index_to_base_year(test_input),
        right=test_expected,
    ) and assert_series_equal(
        left=index_to_base_year(test_input),
        right=test_input,
    ) is False
