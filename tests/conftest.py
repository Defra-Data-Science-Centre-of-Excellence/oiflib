"""Test configuration file for package-level modules."""

from pandas import DataFrame
from pytest import fixture


# ? Would it be better to use mock objects instead of temp files?
# Input and output objects needed to multiple tests
@fixture(scope="module")
def df_input() -> DataFrame:
    """A minimal input DataFrame for testing the extract module.

    Returns:
        DataFrame: A minimal input DataFrame for testing the extract module.
    """
    return DataFrame(
        data={
            1: ["a", "b"],
            2: [4, 5],
            3: [6.7, 8.9],
        },
    )


@fixture(scope="module")
def df_output() -> DataFrame:
    """A minimal output DataFrame for testing the extract module.

    Returns:
        DataFrame: A minimal output DataFrame for testing the extract module.
    """
    return DataFrame(
        data={
            "1": ["a", "b"],
            "2": [4, 5],
            "3": [6.7, 8.9],
        },
    )
