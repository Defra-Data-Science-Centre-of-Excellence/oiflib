"""Tests for oiflib core functions."""
from pandas import Series
from pandas.testing import assert_series_equal

from oiflib.core import index_to_base_year


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
