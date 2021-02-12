"""Test fixtures for Air Four module."""

from pandas import DataFrame
from pytest import fixture


@fixture(scope="module")
def extracted() -> DataFrame:
    """A minimal input DataFrame for testing Air Four enrichment functions."""
    return DataFrame(
        data={
            "Year": [1987, 1988, 1989],
            "Site": ["All sites"] * 3,
            "Annual Mean NO2 concentration (µg/m3)": [
                70.0,
                65.0,
                60.0,
            ],
            "95% confidence interval for 'All sites' (+/-)": [
                7.0,
                6.5,
                6.0,
            ],
        }
    )


@fixture(scope="module")
def enriched() -> DataFrame:
    """An expected output DataFrame for testing Air Four enrichment functions."""
    return DataFrame(
        data={
            "Year": [1987, 1988, 1989],
            "Site": ["All sites"] * 3,
            "Annual Mean NO2 concentration (µg/m3)": [
                70.0,
                65.0,
                60.0,
            ],
            "95% confidence interval for 'All sites' (+/-)": [
                7.0,
                6.5,
                6.0,
            ],
            "Lower_CI_bound": [63.0, 58.5, 54.0],
            "Upper_CI_bound": [77.0, 71.5, 66.0],
        }
    )
