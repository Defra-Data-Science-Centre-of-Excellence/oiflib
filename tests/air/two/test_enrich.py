"""Tests for Air Two enrich module."""
from unittest.mock import MagicMock, patch

from pandas import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.two.enrich import (
    agg_CO2e_by_category_and_year,
    enrich_air_two,
    join_to_lookup,
)


@patch("oiflib.air.two.enrich.read_csv")
def test_join_to_lookup(
    mock_read_csv: MagicMock,
    lookup: DataFrame,
    transformed: DataFrame,
    transformed_joined: DataFrame,
) -> None:
    """OIF categories are joined to the transformed DataFrame."""
    mock_read_csv.return_value = lookup

    returned: DataFrame = join_to_lookup(
        df=transformed,
    )

    expected: DataFrame = transformed_joined

    assert_frame_equal(
        left=returned,
        right=expected,
    )


def test_agg_CO2e_by_category_and_year(
    transformed_joined: DataFrame,
    enriched: DataFrame,
) -> None:
    """CO2e is aggregated by category and EmissionsYear."""
    returned: DataFrame = agg_CO2e_by_category_and_year(transformed_joined)

    expected: DataFrame = enriched

    assert_frame_equal(
        left=returned,
        right=expected,
    )


@patch("oiflib.air.two.enrich.read_csv")
def test_enrich(
    mock_read_csv: MagicMock,
    lookup: DataFrame,
    transformed: DataFrame,
    enriched: DataFrame,
) -> None:
    """OIF categories are joined, CO2e is aggregated by category and EmissionsYear."""
    mock_read_csv.return_value = lookup

    returned: DataFrame = enrich_air_two(transformed)

    expected: DataFrame = enriched

    assert_frame_equal(
        left=returned,
        right=expected,
    )
