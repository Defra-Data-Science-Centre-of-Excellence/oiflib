"""Tests for functions in Air Four DataFrame enrich module."""
# from hypothesis import given
from pandas import DataFrame
from pandas.testing import assert_frame_equal  # , assert_series_equal

from oiflib.air.four.enrich import _add_bounds, enrich_air_four

# from oiflib.air.four.schema import schema_extracted


# class TestProperties:
#     """A class to hold property-based tests."""

#     @given(schema_extracted.strategy(size=1))
#     def test__add_bounds(self, df: DataFrame):
#         output: DataFrame = _add_bounds(df)

#         assert "Lower_CI_bound" and "Upper_CI_bound" in output

#         assert_series_equal(
#             left=output["Lower_CI_bound"],
#             right=(
#                 output["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]  # noqa: B950 - Long variable name
#                 - output["95% confidence interval for 'All sites' (+/-)"]
#             ),
#             check_names=False,
#         )

#         assert_series_equal(
#             left=output["Upper_CI_bound"],
#             right=(
#                 output["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]  # noqa: B950 - Long variable name
#                 + output["95% confidence interval for 'All sites' (+/-)"]
#             ),
#             check_names=False,
#         )


#     @given(schema_extracted.strategy(size=1))
#     def test_enrich_air_four(self, df: DataFrame):
#         output: DataFrame = enrich_air_four(df)

#         assert "Lower_CI_bound" and "Upper_CI_bound" in output

#         assert_series_equal(
#             left=output["Lower_CI_bound"],
#             right=(
#                 output["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]  # noqa: B950 - Long variable name
#                 - output["95% confidence interval for 'All sites' (+/-)"]
#             ),
#             check_names=False,
#         )

#         assert_series_equal(
#             left=output["Upper_CI_bound"],
#             right=(
#                 output["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]  # noqa: B950 - Long variable name
#                 + output["95% confidence interval for 'All sites' (+/-)"]
#             ),
#             check_names=False,
#         )


class TestExamples:
    """A class to hold example-based tests."""

    def test__add_bounds(self, extracted: DataFrame, enriched: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        output: DataFrame = _add_bounds(extracted)

        assert_frame_equal(
            left=output,
            right=enriched,
        )

    def test_enrich_air_four(self, extracted: DataFrame, enriched: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        output: DataFrame = enrich_air_four(extracted)

        assert_frame_equal(
            left=output,
            right=enriched,
        )
