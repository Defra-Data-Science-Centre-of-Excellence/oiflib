"""Tests for push module."""
from pytest_cases import parametrize

from oiflib.push import _set_data_file_name, _write_to_csv


@parametrize(
    "theme, indicator, expected",
    [
        ("air", "one", "indicator_1-1-1.csv"),
        ("international", "four", "indicator_10-4-1.csv"),
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
    expected_air_one_formatted_rename_disaggregation_column,
) -> None:
    """A DataFrame is written to csv."""
    _write_to_csv(
        df=expected_air_one_formatted_rename_disaggregation_column,
        repo="OIF-Dashboard-Data",
        data_folder="data",
        data_file_name="",
    )
    pass
