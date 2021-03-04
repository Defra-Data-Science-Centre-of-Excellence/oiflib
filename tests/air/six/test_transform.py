"""Tests for Air Six Transform module."""
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal

from oiflib.air.six.transform import _unpivot, transform_air_six


def test__unpivot(
    air_six_extracted: DataFrame,
    air_six_transformed: DataFrame,
) -> None:
    """Air Six extarcted is unpivoted."""
    returned: DataFrame = _unpivot(
        df=air_six_extracted,
    )

    assert_frame_equal(
        left=returned,
        right=air_six_transformed,
    )


def test_transform_air_six(
    air_six_extracted: DataFrame,
    air_six_transformed: DataFrame,
) -> None:
    """Air Six extarcted is unpivoted."""
    returned: DataFrame = transform_air_six(
        df=air_six_extracted,
    )

    assert_frame_equal(
        left=returned,
        right=air_six_transformed,
    )
