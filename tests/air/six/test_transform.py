"""Tests for Air Six Transform module."""
from typing import Callable, Dict

from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
from pytest_cases import fixture_ref, parametrize

from oiflib.air.six.transform import _filter, _unpivot, transform_air_six

dict_funcs: Dict[str, Callable[[DataFrame], DataFrame]] = {
    "_filter": _filter,
    "_unpivot": _unpivot,
    "transform_air_six": transform_air_six,
}


@parametrize(
    argnames="input, func_name, expected",
    argvalues=[
        (
            fixture_ref("air_six_unpivoted"),
            "_filter",
            fixture_ref("air_six_transformed"),
        ),
        (
            fixture_ref("air_six_extracted"),
            "_unpivot",
            fixture_ref("air_six_unpivoted"),
        ),
        (
            fixture_ref("air_six_extracted"),
            "transform_air_six",
            fixture_ref("air_six_transformed"),
        ),
    ],
    ids=dict_funcs.keys(),
)
def test_df_funcs(
    input: DataFrame,
    func_name: str,
    expected: DataFrame,
) -> None:
    """An example input returns an expected output."""
    returned: DataFrame = dict_funcs[func_name](input)

    assert_frame_equal(
        left=returned,
        right=expected,
    )
