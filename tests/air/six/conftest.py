"""Example DataFrames for testing Air Six modules."""
from pandas import DataFrame
from pytest import fixture


@fixture
def air_six_extracted() -> DataFrame:
    """Example of Air Six extracted DataFrame."""
    return DataFrame(
        data={
            "Year": ["1995-1997", "1998-2000"],
            "England": [1.2, 3.4],
            "Wales": [5.6, 7.8],
            "Scotland": [9.10, 11.12],
            "NI": [13.14, 15.16],
            "UK": [29.04, 37.48],
        },
    )


@fixture
def air_six_unpivoted() -> DataFrame:
    """Example of Air Six unpivoted DataFrame."""
    return DataFrame(
        data={
            "Year": [
                "1995-1997",
                "1998-2000",
                "1995-1997",
                "1998-2000",
                "1995-1997",
                "1998-2000",
                "1995-1997",
                "1998-2000",
                "1995-1997",
                "1998-2000",
            ],
            "Country": [
                "England",
                "England",
                "Wales",
                "Wales",
                "Scotland",
                "Scotland",
                "NI",
                "NI",
                "UK",
                "UK",
            ],
            "Percentage habitat area exceeded by deposition data": [
                1.20,
                3.40,
                5.60,
                7.80,
                9.10,
                11.12,
                13.14,
                15.16,
                29.04,
                37.48,
            ],
        }
    )


@fixture
def air_six_transformed() -> DataFrame:
    """Example of Air Six transformed DataFrame."""
    return DataFrame(
        data={
            "Year": [
                "1995-1997",
                "1998-2000",
            ],
            "Country": [
                "England",
                "England",
            ],
            "Percentage habitat area exceeded by deposition data": [
                1.20,
                3.40,
            ],
        }
    )
