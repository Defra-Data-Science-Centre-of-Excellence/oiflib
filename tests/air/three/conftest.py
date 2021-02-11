"""Common variables for Air Three Tests."""
from pathlib import Path
from typing import List

from pandas import DataFrame
from pytest import fixture


@fixture(autouse=True)
def input_2000() -> DataFrame:
    """Example input DataFrame for CSV one."""
    return DataFrame(
        data={
            "Area code": [
                None,
                None,
                "Area code",
                "Eng",
                "Wal",
            ],
            "PM2.5 2000 (total)": [
                None,
                None,
                "PM2.5 2000 (total)",
                4.6,
                9.0,
            ],
            "PM2.5 2000 (non-anthropogenic)": [
                None,
                None,
                "PM2.5 2000 (non-anthropogenic)",
                0.1,
                2.3,
            ],
            "PM2.5 2000 (anthropogenic)": [
                None,
                None,
                "PM2.5 2000 (anthropogenic)",
                4.5,
                6.7,
            ],
            "Country": [
                None,
                None,
                "Country",
                "England",
                "Wales",
            ],
        }
    )


@fixture(autouse=True)
def input_2001() -> DataFrame:
    """Example input DataFrame for CSV two."""
    return DataFrame(
        data={
            "Area code": [
                None,
                None,
                "Area code",
                "Eng",
                "Wal",
            ],
            "PM2.5 2001 (total)": [
                None,
                None,
                "PM2.5 2001 (total)",
                21.03,
                24.26,
            ],
            "PM2.5 2001 (non-anthropogenic)": [
                None,
                None,
                "PM2.5 2001 (non-anthropogenic)",
                8.9,
                10.11,
            ],
            "PM2.5 2001 (anthropogenic)": [
                None,
                None,
                "PM2.5 2001 (anthropogenic)",
                12.13,
                14.15,
            ],
            "Country": [
                None,
                None,
                "Country",
                "England",
                "Wales",
            ],
        }
    )


@fixture(autouse=True)
def inputs(input_2000, input_2001) -> List[DataFrame]:
    """List of example input DataFrames."""
    return [input_2000, input_2001]


@fixture(autouse=True)
def write_inputs_to_temporary_directory(
    tmp_path: Path,
    inputs: List[DataFrame],
) -> None:
    """Writes a list of example input DataFrame to temporary CSV files."""
    for count, value in enumerate(inputs):
        path = tmp_path / f"test_200{ str(count) }.csv"

        path.touch()

        value.to_csv(
            path_or_buf=path,
            float_format="%.2f",
            index=False,
            header=False,
        )


@fixture
def extracted() -> DataFrame:
    """Expected output of extract function given example inputs."""
    return DataFrame(
        data={
            "Area code": ["Eng", "Wal"],
            "PM2.5 2000 (total)": [4.6, 9.0],
            "PM2.5 2000 (non-anthropogenic)": [0.1, 2.3],
            "PM2.5 2000 (anthropogenic)": [4.5, 6.7],
            "Country": ["England", "Wales"],
            "PM2.5 2001 (total)": [21.03, 24.26],
            "PM2.5 2001 (non-anthropogenic)": [8.9, 10.11],
            "PM2.5 2001 (anthropogenic)": [12.13, 14.15],
        }
    )


@fixture
def extracted_unpivoted() -> DataFrame:
    """Expected output of unpivot function given extracted."""
    return DataFrame(
        data={
            "Area code": [
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
            ],
            "Country": [
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
            ],
            "variable": [
                "PM2.5 2000 (total)",
                "PM2.5 2000 (total)",
                "PM2.5 2000 (non-anthropogenic)",
                "PM2.5 2000 (non-anthropogenic)",
                "PM2.5 2000 (anthropogenic)",
                "PM2.5 2000 (anthropogenic)",
                "PM2.5 2001 (total)",
                "PM2.5 2001 (total)",
                "PM2.5 2001 (non-anthropogenic)",
                "PM2.5 2001 (non-anthropogenic)",
                "PM2.5 2001 (anthropogenic)",
                "PM2.5 2001 (anthropogenic)",
            ],
            "ugm-3": [
                4.6,
                9.0,
                0.1,
                2.3,
                4.5,
                6.7,
                21.03,
                24.26,
                8.9,
                10.11,
                12.13,
                14.15,
            ],
        }
    )


@fixture
def extracted_split() -> DataFrame:
    """Expected output of split function given extracted_unpivot."""
    return DataFrame(
        data={
            "Area code": [
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
            ],
            "Country": [
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
            ],
            "variable": [
                "PM2.5 2000 (total)",
                "PM2.5 2000 (total)",
                "PM2.5 2000 (non-anthropogenic)",
                "PM2.5 2000 (non-anthropogenic)",
                "PM2.5 2000 (anthropogenic)",
                "PM2.5 2000 (anthropogenic)",
                "PM2.5 2001 (total)",
                "PM2.5 2001 (total)",
                "PM2.5 2001 (non-anthropogenic)",
                "PM2.5 2001 (non-anthropogenic)",
                "PM2.5 2001 (anthropogenic)",
                "PM2.5 2001 (anthropogenic)",
            ],
            "ugm-3": [
                4.6,
                9.0,
                0.1,
                2.3,
                4.5,
                6.7,
                21.03,
                24.26,
                8.9,
                10.11,
                12.13,
                14.15,
            ],
            "year": [
                "2000",
                "2000",
                "2000",
                "2000",
                "2000",
                "2000",
                "2001",
                "2001",
                "2001",
                "2001",
                "2001",
                "2001",
            ],
            "measure": [
                "total",
                "total",
                "non-anthropogenic",
                "non-anthropogenic",
                "anthropogenic",
                "anthropogenic",
                "total",
                "total",
                "non-anthropogenic",
                "non-anthropogenic",
                "anthropogenic",
                "anthropogenic",
            ],
        }
    )


@fixture
def extracted_selected() -> DataFrame:
    """Expected output of select function given extracted_split."""
    return DataFrame(
        data={
            "Area code": [
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
                "Eng",
                "Wal",
            ],
            "Country": [
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
                "England",
                "Wales",
            ],
            "year": [
                "2000",
                "2000",
                "2000",
                "2000",
                "2000",
                "2000",
                "2001",
                "2001",
                "2001",
                "2001",
                "2001",
                "2001",
            ],
            "measure": [
                "total",
                "total",
                "non-anthropogenic",
                "non-anthropogenic",
                "anthropogenic",
                "anthropogenic",
                "total",
                "total",
                "non-anthropogenic",
                "non-anthropogenic",
                "anthropogenic",
                "anthropogenic",
            ],
            "ugm-3": [
                4.6,
                9.0,
                0.1,
                2.3,
                4.5,
                6.7,
                21.03,
                24.26,
                8.9,
                10.11,
                12.13,
                14.15,
            ],
        }
    )


@fixture
def transformed() -> DataFrame:
    """Expected output of transform function given extracted."""
    return DataFrame(
        data={
            "Area code": [
                "Eng",
                "Eng",
            ],
            "Country": [
                "England",
                "England",
            ],
            "year": [
                "2000",
                "2001",
            ],
            "measure": [
                "total",
                "total",
            ],
            "ugm-3": [
                4.6,
                21.03,
            ],
        }
    )
