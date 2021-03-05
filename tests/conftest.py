"""Test configuration file for package-level modules."""
# Standard library imports
from json import dump
from typing import Any, Dict

# Third party imports
import pytest
from _pytest.tmpdir import TempPathFactory
from pandas import DataFrame


# Input and output objects needed to multiple tests
@pytest.fixture(scope="module")
def df_input() -> DataFrame:
    """A minimal input DataFrame for testing the extract module."""
    return DataFrame(
        data={
            1: ["a", "b"],
            2: [4, 5],
            3: [6.7, 8.9],
        },
    )


@pytest.fixture(scope="module")
def df_output() -> DataFrame:
    """A minimal output DataFrame for testing the extract module."""
    return DataFrame(
        data={
            "1": ["a", "b"],
            "2": [4, 5],
            "3": [6.7, 8.9],
        },
    )


@pytest.fixture(scope="module")
def file_xlsx(tmp_path_factory: TempPathFactory, df_input: DataFrame) -> str:
    """Writes a DataFrame to a temporary Excel file, returns the path as a string."""
    path = tmp_path_factory.getbasetemp() / "test.xlsx"

    path_as_string: str = str(path)

    df_input.to_excel(
        excel_writer=path_as_string,
        sheet_name="Sheet",
        startrow=5,
        startcol=5,
        index=False,
        float_format="%.2f",
    )

    return path_as_string


@pytest.fixture(scope="module")
def kwargs_input(file_xlsx: str) -> Dict[str, Any]:
    """Returns a dictionary of key word arguments to be passed to read_excel()."""
    return {
        "io": file_xlsx,
        "sheet_name": "Sheet",
        "usecols": [5, 6, 7],
        "skiprows": 5,
        "nrows": 2,
    }


@pytest.fixture(scope="module")
def dictionary_input(
    kwargs_input: Dict[str, Any]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Returns a minimal data dictionary for testing the extract module."""
    return {
        "theme": {
            "indicator": kwargs_input,
        },
    }


@pytest.fixture(scope="module")
def file_json(
    tmp_path_factory: TempPathFactory,
    dictionary_input: Dict[str, Dict[str, Dict[str, Any]]],
) -> str:
    """Converts dict to JSON object, writes to temp file, returns path as string."""
    path = tmp_path_factory.getbasetemp() / "test.json"

    path_as_string: str = str(path)

    with open(path_as_string, "w") as file:
        dump(dictionary_input, file)

    return path_as_string
