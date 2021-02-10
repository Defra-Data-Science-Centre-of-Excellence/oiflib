"""Tests for Air Two transform module."""
from typing import Dict

# Third-party Libraries
from hypothesis import HealthCheck, assume, given, settings
from pandas import DataFrame, Series
from pandas.core.arrays.string_ import StringArray
from pandas.testing import assert_frame_equal
from pandera import DataFrameSchema
from pytest import fixture, raises

# Local libraries
from oiflib.air.two.transform import (
    drop_BaseYear_column,
    forward_fill_NCFormat_column,
    transform_air_two,
    unpivot,
)
from oiflib.validate import _dict_from_path, _schema_from_dict


def get_schema_extracted() -> DataFrameSchema:
    """Returns Air Two Extracted DataFrameScheme from pickled dict."""
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]] = _dict_from_path(
        file_path="/home/edfawcetttaylor/repos/oiflib/data/schema.pkl",
    )

    schema: DataFrameSchema = _schema_from_dict(
        dict=dict,
        theme="air",
        indicator="two",
        stage="extracted",
    )

    return schema


schema_extracted: DataFrameSchema = get_schema_extracted()


class TestProperties:
    """A class to hold property-based tests."""

    @given(schema_extracted.strategy(size=5))
    @settings(suppress_health_check=[HealthCheck.too_slow])
    def test_drop_BaseYear_column(self, df: DataFrame) -> None:
        """BaseYear column is dropped.

        Args:
            df (DataFrame): A DataFrame generated from schema_extracted.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        output: DataFrame = drop_BaseYear_column(df)
        assert "BaseYear" not in output

    @given(schema_extracted.strategy(size=5))
    @settings(suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])
    def test_forward_fill_NCFormat_column(self, df: DataFrame) -> None:
        """NCFormat doesn't contain NaNs.

        This test assumes that the first value NCFormat is "Agriculture" as, if the
        first value is NaN, the function will raise an error and fail.

        Args:
            df (DataFrame): A DataFrame generated from schema_extracted.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        assume(df.NCFormat[0] == "Agriculture")
        output: DataFrame = forward_fill_NCFormat_column(df)
        series: Series = output.NCFormat
        string_array: StringArray = series.isna().array
        assert not string_array.any()

    @given(schema_extracted.strategy(size=5))
    @settings(suppress_health_check=[HealthCheck.too_slow])
    def test_unpivot(self, df: DataFrame) -> None:
        """The unpivoted DataFrame will have 10 rows and 4 columns.

        The input DataFrame, which is generated from schema_extracted, will have
        5 rows and 4 columns so, when unpivoted, it will have 10 rows and 4
        columns.

        Args:
            df (DataFrame): A DataFrame generated from schema_extracted.
        """
        output: DataFrame = unpivot(df)
        assert output.shape == (10, 4)


class TestExamples:
    """A class to hold example-based tests."""

    @fixture
    def df_input(self) -> DataFrame:
        """Creates a minimal DataFrame for testing Air Two transform functions.

        Returns:
            DataFrame: A minimal DataFrame for testing Air Two transform
            functions.
        """
        return DataFrame(
            data={
                "NCFormat": [
                    "Agriculture",
                    None,
                    "Agriculture Total",
                ],
                "IPCC": [
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                ],
                "BaseYear": [
                    float(0),
                    float(1),
                    float(2),
                ],
                "1990": [
                    float(3),
                    float(4),
                    float(5),
                ],
            },
        )

    @fixture
    def df_input_missing(self, df_input: DataFrame) -> DataFrame:
        """df_input but with the first NCFormat value removed."""
        df: DataFrame = df_input

        df["NCFormat"][0] = None

        return df

    def test_drop_BaseYear_column(self, df_input: DataFrame) -> None:
        """BaseYear column is dropped.

        Args:
            df_input (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        df_output_expected: DataFrame = DataFrame(
            data={
                "NCFormat": [
                    "Agriculture",
                    None,
                    "Agriculture Total",
                ],
                "IPCC": [
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                ],
                "1990": [
                    float(3),
                    float(4),
                    float(5),
                ],
            },
        )

        df_output_received: DataFrame = drop_BaseYear_column(df=df_input)

        assert_frame_equal(left=df_output_expected, right=df_output_received)

    def test_forward_fill_NCFormat_column_pass(self, df_input: DataFrame) -> None:
        """NaNs in the NCFormat column are replaced with the preceding non-NaN value.

        Args:
            df_input (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        df_output_expected: DataFrame = DataFrame(
            data={
                "NCFormat": [
                    "Agriculture",
                    "Agriculture",
                    "Agriculture Total",
                ],
                "IPCC": [
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                ],
                "BaseYear": [
                    float(0),
                    float(1),
                    float(2),
                ],
                "1990": [
                    float(3),
                    float(4),
                    float(5),
                ],
            },
        )

        df_output_received: DataFrame = forward_fill_NCFormat_column(df=df_input)

        assert_frame_equal(left=df_output_expected, right=df_output_received)

    def test_forward_fill_NCFormat_column_fail(
        self, df_input_missing: DataFrame
    ) -> None:
        """Dummy docstring."""
        with raises(ValueError):
            forward_fill_NCFormat_column(df=df_input_missing)

    def test_unpivot(self, df_input: DataFrame) -> None:
        """BaseYear and 1990 are unpivoted in to EmissionsYear and CO2 Equiv.

        Args:
            df_input (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        df_output_expected: DataFrame = DataFrame(
            data={
                "NCFormat": [
                    "Agriculture",
                    None,
                    "Agriculture Total",
                    "Agriculture",
                    None,
                    "Agriculture Total",
                ],
                "IPCC": [
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                ],
                "EmissionYear": [
                    "BaseYear",
                    "BaseYear",
                    "BaseYear",
                    "1990",
                    "1990",
                    "1990",
                ],
                "CO2 Equiv": [
                    float(0),
                    float(1),
                    float(2),
                    float(3),
                    float(4),
                    float(5),
                ],
            },
        )

        df_output_received: DataFrame = unpivot(df=df_input)

        assert_frame_equal(left=df_output_expected, right=df_output_received)

    def test_transform_air_two(self, df_input: DataFrame) -> None:
        """BaseYear dropped, NCFormat filled from above, BaseYear & 1990 unpivoted.

        - BaseYear column is dropped.
        - NaNs in the NCFormat Series are replaced with the preceding non-NaN value.
        - BaseYear and 1990 are unpivoted in to EmissionsYear and CO2 Equiv.

        Args:
            df_input (DataFrame): A minimal DataFrame for testing Air Two transform
                functions.
        """  # noqa - D403: "Proper capitalisation of first line" isn't appropriate
        df_output_expected: DataFrame = DataFrame(
            data={
                "NCFormat": [
                    "Agriculture",
                    "Agriculture",
                    "Agriculture Total",
                ],
                "IPCC": [
                    "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                    "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                    None,
                ],
                "EmissionYear": [
                    "1990",
                    "1990",
                    "1990",
                ],
                "CO2 Equiv": [
                    float(3),
                    float(4),
                    float(5),
                ],
            },
        )

        df_output_received: DataFrame = transform_air_two(df=df_input)

        assert_frame_equal(left=df_output_expected, right=df_output_received)
