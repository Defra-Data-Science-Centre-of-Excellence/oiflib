"""Tests for the Air Three extract module."""
from pathlib import Path

from pandas import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.three.extract import extract_air_three


def test_extract_air_three(tmp_path: Path, extracted: DataFrame) -> None:
    """Multiple files are extracted and joined them into a single DataFrame."""
    file_path_base: str = str(tmp_path / "test_YYYY.csv")

    expected: DataFrame = extracted

    output = extract_air_three(
        file_path_base=file_path_base,
        range_start=2000,
        range_end=2001,
        join_on=["Area code", "Country"],
    )

    assert_frame_equal(
        left=output,
        right=expected,
    )
