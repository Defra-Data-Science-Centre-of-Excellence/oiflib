"""Tests for oiflib.air.one.enrich module."""

# Third-party Libraries
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest import fixture

# Local libraries
from oiflib.air.one.enrich import enrich_air_one, index_emission_to_base_year


@fixture
def test_input_air_one_enrich() -> DataFrame:
    """Creates a minimal input example DataFrame for testing Air One enrich functions.

    Returns:
        DataFrame: A minimal input example DataFrame for testing Air One enrich
        functions.
    """
    return DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"] * 2,
            "Year": [1990] * 5 + [1991] * 5,
            "Emissions": [2] * 5 + [3] * 5,
        },
    )


@fixture
def test_expected_air_one_enrich() -> DataFrame:
    """Creates a minimal output example DataFrame for testing Air One enrich functions.

    Returns:
        DataFrame: A minimal output example DataFrame for testing Air One enrich
        functions.
    """
    return DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"] * 2,
            "Year": [1990] * 5 + [1991] * 5,
            "Emissions": [2] * 5 + [3] * 5,
            "Index": [2 / 2 * 100] * 5 + [3 / 2 * 100] * 5,
        },
    )


def test_index_to_base_year(
    test_input_air_one_enrich: DataFrame,
    test_expected_air_one_enrich: DataFrame,
):
    """An "Index" column is added with "Emission" values indexed to base year."""
    assert_frame_equal(
        left=test_input_air_one_enrich.pipe(index_emission_to_base_year),
        right=test_expected_air_one_enrich,
    ) and assert_frame_equal(
        left=test_input_air_one_enrich.pipe(index_emission_to_base_year),
        right=test_input_air_one_enrich,
    ) is False


def test_enrich_air_one(
    test_input_air_one_enrich: DataFrame,
    test_expected_air_one_enrich: DataFrame,
):
    """An "Index" column is added with "Emission" values indexed to base year."""
    assert_frame_equal(
        left=enrich_air_one(test_input_air_one_enrich),
        right=test_expected_air_one_enrich,
    ) and assert_frame_equal(
        left=enrich_air_one(test_input_air_one_enrich),
        right=test_input_air_one_enrich,
    ) is False
