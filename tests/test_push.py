"""Tests for push module."""
from pathlib import Path

from pandas import read_csv
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
from pytest_cases import parametrize

from oiflib.push import _set_data_file_name, _write_to_csv


@parametrize(
    "theme, indicator, expected",
    [
        ("air", "one", "indicator_1-1-1.csv"),
        ("international", "four", "indicator_0-0-4.csv"),
    ],
)
def test__set_data_file_name(theme, indicator, expected) -> None:
    """A filename is returned."""
    returned: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    assert returned == expected


def test__write_to_csv(
    tmp_path: Path,
    expected_air_one_formatted_rename_disaggregation_column: DataFrame,
) -> None:
    """A DataFrame is written to csv."""
    test_repo: Path = tmp_path / "test-repo"

    test_repo.mkdir()

    test_folder: Path = test_repo / "test"

    test_folder.mkdir()

    file_path: str = _write_to_csv(
        df=expected_air_one_formatted_rename_disaggregation_column,
        root=tmp_path,
        repo="test-repo",
        data_folder="test",
        data_file_name="test.csv",
    )

    written: DataFrame = read_csv(
        filepath_or_buffer=file_path,
    )

    assert_frame_equal(
        left=written,
        right=expected_air_one_formatted_rename_disaggregation_column,
    )
