"""Tests for format module functions."""

from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest_cases import fixture_ref, parametrize

from oiflib.format import format


@parametrize(
    "additional_args, expected",
    [
        (
            fixture_ref("kwargs_no_disaggregation_column"),
            fixture_ref("expected_air_one_formatted_no_disaggregation_column"),
        ),
        (
            fixture_ref("kwargs_disaggregation_column"),
            fixture_ref("expected_air_one_formatted_disaggregation_column"),
        ),
        (
            fixture_ref("kwargs_rename_disaggregation_column"),
            fixture_ref("expected_air_one_formatted_rename_disaggregation_column"),
        ),
    ],
)
def test_format(
    expected_air_one_enriched,
    additional_args,
    expected,
) -> None:
    """The Dataframe is formatted for OpenSDG."""
    returned: DataFrame = format(
        df=expected_air_one_enriched,
        year_column="EmissionYear",
        value_column="Index",
        **additional_args,
    )

    assert_frame_equal(
        left=returned,
        right=expected,
    )
