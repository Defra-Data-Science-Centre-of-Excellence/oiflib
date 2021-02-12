"""Tests for functions in Air Five DataFrame enrich module."""
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.five.enrich import _add_bounds, enrich_air_five


class TestExamples:
    """A class to hold example-based tests."""

    def test__add_bounds(self, extracted: DataFrame, enriched: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        output: DataFrame = _add_bounds(extracted)

        assert_frame_equal(
            left=output,
            right=enriched,
        )

    def test_enrich_air_five(self, extracted: DataFrame, enriched: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        output: DataFrame = enrich_air_five(extracted)

        assert_frame_equal(
            left=output,
            right=enriched,
        )
