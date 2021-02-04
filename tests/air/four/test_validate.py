"""Tests for functions in Air Four DataFrame validate module."""

from pandas import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.four.validate import (  # noqa: I001; noqa: I001, I005
    validate_air_four_enriched,
    validate_air_four_extracted,
)

# noqa: I005


def test_validate_air_four_extracted(extracted: DataFrame):
    """A valid DataFrame returns itself."""
    output: DataFrame = validate_air_four_extracted(extracted)

    assert_frame_equal(
        left=output,
        right=extracted,
    )


def test_validate_air_four_enriched(enriched: DataFrame):
    """A valid DataFrame returns itself."""
    output: DataFrame = validate_air_four_enriched(enriched)

    assert_frame_equal(
        left=output,
        right=enriched,
    )
