"""Air Two test configuration."""
from pandas import DataFrame
from pytest import fixture


@fixture
def df_input() -> DataFrame:
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
def transformed() -> DataFrame:
    """A minimal example of a transformed Air Two DataFrame."""
    return DataFrame(
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


@fixture
def lookup() -> DataFrame:
    """A minimal example of a look up for the Air Two DataFrame."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                "Agriculture",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
            ],
            "OIF_category": [
                "Agriculture",
                "Agriculture",
            ],
        }
    )


@fixture
def transformed_joined() -> DataFrame:
    """The expected output of joining the transformed and lookup DataFrames."""
    return DataFrame(
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
            "OIF_category": [
                "Agriculture",
                "Agriculture",
                None,
            ],
        },
    )


@fixture
def enriched() -> DataFrame:
    """A minimal example of an enriched Two DataFrame."""
    return DataFrame(
        data={
            "OIF_category": [
                "Agriculture",
            ],
            "EmissionYear": [
                "1990",
            ],
            "CO2 Equiv": [
                float(7),
            ],
        },
    )
