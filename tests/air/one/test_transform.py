"""Tests for oiflib.air.one.transform module."""

# Third-party Libraries
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest import fixture

# Local libraries
from oiflib.air.one.transform import (
    clean_column_values,
    drop_columns,
    filter_rows,
    transform_air_one,
    unpivot,
)


# Define left DataFrame as fixture for use in multiple tests
@fixture
def df_input() -> DataFrame:
    """Creates a minimal DataFrame for testing oiflib.air.one.transform functions.

    Returns:
        DataFrame: A minimal DataFrame for testing oiflib.air.one.transform
        functions.
    """
    return DataFrame(
        data={
            "ShortPollName": [
                "NH3 Total",
                "NOx Total",
                "SO2 Total",
                "VOC Total",
                "PM2.5 Total",
                "Another Total",
            ],
            "NFRCode": [0] * 6,
            "SourceName": [0] * 6,
            "Value": [1, 2, 3, 4, 5, 6],
        },
    )


def test_filter_rows(df_input: DataFrame):
    """Only the total rows of the five pollutants are returned."""
    # Create expected output
    df_output_expected: DataFrame = DataFrame(
        data={
            "ShortPollName": [
                "NH3 Total",
                "NOx Total",
                "SO2 Total",
                "VOC Total",
                "PM2.5 Total",
            ],
            "NFRCode": [0] * 5,
            "SourceName": [0] * 5,
            "Value": [1, 2, 3, 4, 5],
        },
    )

    # Apply function to input
    df_output_actual: DataFrame = filter_rows(df=df_input)

    assert_frame_equal(df_output_expected, df_output_actual)


def test_drop_columns(df_input: DataFrame):
    """NCRCode and SourceName are dropped."""  # noqa - proper capitalisation is not applicable
    # Create expected output
    df_output_expected: DataFrame = DataFrame(
        data={
            "ShortPollName": [
                "NH3 Total",
                "NOx Total",
                "SO2 Total",
                "VOC Total",
                "PM2.5 Total",
                "Another Total",
            ],
            "Value": [1, 2, 3, 4, 5, 6],
        },
    )

    # Apply function to input
    df_output_actual: DataFrame = drop_columns(df=df_input)

    assert_frame_equal(df_output_expected, df_output_actual)


def test_clean_column_values(df_input: DataFrame):
    """Total is removed from pollutant names and NM is prefixed to VOC."""
    # Create expected output
    df_output_expected: DataFrame = DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5", "Another"],
            "NFRCode": [0] * 6,
            "SourceName": [0] * 6,
            "Value": [1, 2, 3, 4, 5, 6],
        },
    )

    # Apply function to input
    df_output_actual: DataFrame = clean_column_values(df=df_input)

    assert_frame_equal(df_output_expected, df_output_actual)


def test_unpivot(df_input: DataFrame):
    """Unpivots the input as expected."""
    # Create expected output
    df_output_expected: DataFrame = DataFrame(
        data={
            "ShortPollName": [
                "NH3 Total",
                "NOx Total",
                "SO2 Total",
                "VOC Total",
                "PM2.5 Total",
                "Another Total",
            ]
            * 3,
            "EmissionYear": ["NFRCode"] * 6 + ["SourceName"] * 6 + ["Value"] * 6,
            "Emission": [0] * 6 + [0] * 6 + [1, 2, 3, 4, 5, 6],
        },
    )

    # Apply function to input
    df_output_actual: DataFrame = unpivot(df=df_input)

    assert_frame_equal(df_output_expected, df_output_actual)


def test_transform_air_one(df_input: DataFrame):
    """Air one input data is filtered, cleaned, and unpivoted."""
    # Create expected output
    df_output_expected: DataFrame = DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"],
            "EmissionYear": ["Value"] * 5,
            "Emission": [1, 2, 3, 4, 5],
        },
    )

    # Apply function to input
    df_output_actual: DataFrame = transform_air_one(df=df_input)

    assert_frame_equal(df_output_expected, df_output_actual)
