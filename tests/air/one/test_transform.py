"""Tests for oiflib.air.one.transform module."""

# Third-party Libraries
import pandas as pd
import pytest

# Local libraries
from oiflib.air.one.transform import (
    clean_column_values,
    drop_columns,
    filter_rows,
    transform_air_one,
    unpivot,
)


# Define left DataFrame as fixture for use in multiple tests
@pytest.fixture
def df_input() -> pd.DataFrame:
    """Creates a minimal DataFrame for testing oiflib.air.one.transform functions.

    Returns:
        pd.DataFrame: A minimal DataFrame for testing oiflib.air.one.transform
        functions.
    """
    return pd.DataFrame(
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


def test_filter_rows(df_input: pd.DataFrame):
    """Only the total rows of the five pollutants are returned."""
    # Create expected output
    df_output_expected: pd.DataFrame = pd.DataFrame(
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
    df_output_actual: pd.DataFrame = filter_rows(df=df_input)

    pd.testing.assert_frame_equal(df_output_expected, df_output_actual)


def test_drop_columns(df_input: pd.DataFrame):
    """NCRCode and SourceName are dropped."""
    # Create expected output
    df_output_expected: pd.DataFrame = pd.DataFrame(
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
    df_output_actual: pd.DataFrame = drop_columns(df=df_input)

    pd.testing.assert_frame_equal(df_output_expected, df_output_actual)


def test_clean_column_values(df_input: pd.DataFrame):
    """Total is removed from pollutant names and NM is prefixed to VOC."""
    # Create expected output
    df_output_expected: pd.DataFrame = pd.DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5", "Another"],
            "NFRCode": [0] * 6,
            "SourceName": [0] * 6,
            "Value": [1, 2, 3, 4, 5, 6],
        },
    )

    # Apply function to input
    df_output_actual: pd.DataFrame = clean_column_values(df=df_input)

    pd.testing.assert_frame_equal(df_output_expected, df_output_actual)


def test_unpivot(df_input: pd.DataFrame):
    """Unpivots the input as expected."""
    # Create expected output
    df_output_expected: pd.DataFrame = pd.DataFrame(
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
            "Year": ["NFRCode"] * 6 + ["SourceName"] * 6 + ["Value"] * 6,
            "Emissions": [0] * 6 + [0] * 6 + [1, 2, 3, 4, 5, 6],
        },
    )

    # Apply function to input
    df_output_actual: pd.DataFrame = unpivot(df=df_input)

    pd.testing.assert_frame_equal(df_output_expected, df_output_actual)


def test_transform_air_one(df_input: pd.DataFrame):
    """Air one input data is filtered, cleaned, and unpivoted."""
    # Create expected output
    df_output_expected: pd.DataFrame = pd.DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"],
            "Year": ["Value"] * 5,
            "Emissions": [1, 2, 3, 4, 5],
        },
    )

    # Apply function to input
    df_output_actual: pd.DataFrame = transform_air_one(df=df_input)

    pd.testing.assert_frame_equal(df_output_expected, df_output_actual)
