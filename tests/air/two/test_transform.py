"""Tests for Air Two transform module."""
# Third-party Libraries
# from hypothesis import HealthCheck, assume, given, settings
from pandas import DataFrame  # , Series

# from pandas.core.arrays.string_ import StringArray
from pandas.testing import assert_frame_equal

# from pandera import DataFrameSchema
from pytest import raises  # fixture,

# Local libraries
from oiflib.air.two.transform import (
    drop_BaseYear_column,
    forward_fill_NCFormat_column,
    transform_air_two,
    unpivot,
)

# class TestProperties:
#     """A class to hold property-based tests."""

#     @fixture
#     def schema(self, schema) -> DataFrameSchema:
#         """Return schema so it can be used by @given()."""
#         return schema

#     schema_extracted: DataFrameSchema = schema()

#     @given(schema_extracted.strategy(size=5))
#     @settings(suppress_health_check=[HealthCheck.too_slow])
#     def test_drop_BaseYear_column(self, df: DataFrame) -> None:
#         """BaseYear column is dropped.

#         Args:
#             df (DataFrame): A DataFrame generated from schema_extracted.
#         """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
#         output: DataFrame = drop_BaseYear_column(df)
#         assert "BaseYear" not in output

#     @given(schema_extracted.strategy(size=5))
#     @settings(suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])  # noqa: B950 - caused by comment
#     def test_forward_fill_NCFormat_column(self, df: DataFrame) -> None:
#         """NCFormat doesn't contain NaNs.

#         This test assumes that the first value NCFormat is "Agriculture" as, if the
#         first value is NaN, the function will raise an error and fail.

#         Args:
#             df (DataFrame): A DataFrame generated from schema_extracted.
#         """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
#         assume(df.NCFormat[0] == "Agriculture")
#         output: DataFrame = forward_fill_NCFormat_column(df)
#         series: Series = output.NCFormat
#         string_array: StringArray = series.isna().array
#         assert not string_array.any()

#     @given(schema_extracted.strategy(size=5))
#     @settings(suppress_health_check=[HealthCheck.too_slow])
#     def test_unpivot(self, df: DataFrame) -> None:
#         """The unpivoted DataFrame will have 10 rows and 4 columns.

#         The input DataFrame, which is generated from schema_extracted, will have
#         5 rows and 4 columns so, when unpivoted, it will have 10 rows and 4
#         columns.

#         Args:
#             df (DataFrame): A DataFrame generated from schema_extracted.
#         """
#         output: DataFrame = unpivot(df)
#         assert output.shape == (10, 4)


class TestExamples:
    """A class to hold example-based tests."""

    def test_drop_BaseYear_column(
        self,
        extracted: DataFrame,
        extracted_dropped: DataFrame,
    ) -> None:
        """BaseYear column is dropped.

        Args:
            extracted (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate

        output: DataFrame = drop_BaseYear_column(df=extracted)

        assert_frame_equal(left=output, right=extracted_dropped)

    def test_forward_fill_NCFormat_column_pass(
        self,
        extracted_dropped: DataFrame,
        extracted_filled: DataFrame,
    ) -> None:
        """NaNs in the NCFormat column are replaced with the preceding non-NaN value.

        Args:
            extracted (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate

        output: DataFrame = forward_fill_NCFormat_column(df=extracted_dropped)

        assert_frame_equal(left=output, right=extracted_filled)

    def test_forward_fill_NCFormat_column_fail(
        self,
        extracted_dropped_missing: DataFrame,
    ) -> None:
        """If the first value in NCFormat is missing, an exception is raised."""
        with raises(ValueError):
            forward_fill_NCFormat_column(df=extracted_dropped_missing)

    def test_unpivot(
        self,
        extracted_filled: DataFrame,
        transformed: DataFrame,
    ) -> None:
        """BaseYear and 1990 are unpivoted in to EmissionsYear and CO2 Equiv.

        Args:
            extracted (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate

        output: DataFrame = unpivot(df=extracted_filled)

        assert_frame_equal(left=output, right=transformed)

    def test_transform_air_two(
        self,
        extracted: DataFrame,
        transformed: DataFrame,
    ) -> None:
        """BaseYear dropped, NCFormat filled from above, BaseYear & 1990 unpivoted.

        - BaseYear column is dropped.
        - NaNs in the NCFormat Series are replaced with the preceding non-NaN value.
        - BaseYear and 1990 are unpivoted in to EmissionsYear and CO2 Equiv.

        Args:
            extracted (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate

        df_output_received: DataFrame = transform_air_two(df=extracted)

        assert_frame_equal(left=transformed, right=df_output_received)
