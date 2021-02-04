"""Tests for functions in Air Four DataFrame enrich module."""
# from hypothesis import given
from pandas import DataFrame
from pandas.testing import assert_frame_equal  # , assert_series_equal
from pytest import fixture

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

    @fixture
    def df_input(self) -> DataFrame:
        """A minimal input DataFrame for testing Air Four enrichment functions."""
        return DataFrame(
            data={
                "Year": [1987, 1988, 1989, 1990, 1991],
                "Site": ["All sites"] * 5,
                "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)": [
                    80.0,
                    75.0,
                    70.0,
                    65.0,
                    60.0,
                ],
                "95% confidence interval for 'All sites' (+/-)": [
                    8.0,
                    7.5,
                    7.0,
                    6.5,
                    6.0,
                ],
            }
        )

    @fixture
    def df_output_expected(self) -> DataFrame:
        """An expected output DataFrame for testing Air Four enrichment functions."""
        return DataFrame(
            data={
                "Year": [1987, 1988, 1989, 1990, 1991],
                "Site": ["All sites"] * 5,
                "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)": [
                    80.0,
                    75.0,
                    70.0,
                    65.0,
                    60.0,
                ],
                "95% confidence interval for 'All sites' (+/-)": [
                    8.0,
                    7.5,
                    7.0,
                    6.5,
                    6.0,
                ],
                "Lower_CI_bound": [72.0, 67.5, 63.0, 58.5, 54.0],
                "Upper_CI_bound": [88.0, 82.5, 77.0, 71.5, 66.0],
            }
        )

    def test__add_bounds(self, df_input: DataFrame, df_output_expected: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        df_output_actual: DataFrame = _add_bounds(df_input)

        assert_frame_equal(
            left=df_output_actual,
            right=df_output_expected,
        )

    def test_enrich_air_four(self, df_input: DataFrame, df_output_expected: DataFrame):
        """Lower/Upper bounds equal annual average -/+ CI."""
        df_output_actual: DataFrame = enrich_air_four(df_input)

        assert_frame_equal(
            left=df_output_actual,
            right=df_output_expected,
        )
