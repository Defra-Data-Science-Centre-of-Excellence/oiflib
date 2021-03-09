"""Tests for oiflib core functions."""
from contextlib import nullcontext as does_not_raise
from typing import Optional

from _pytest._code.code import ExceptionInfo
from pandas import Series
from pandas.testing import assert_series_equal
from pytest import raises
from pytest_cases import parametrize

from oiflib._helper import _check_s3_or_local, _index_to_base_year


def test__index_to_base_year():
    """Each item has been divided by the first item, then multipied by 100."""
    test_input: Series = Series(
        [2.0, 3.0, 4.0, 5.0, 6.0],
    )

    test_expected: Series = Series(
        [
            test_input[0] / test_input[0] * 100,
            test_input[1] / test_input[0] * 100,
            test_input[2] / test_input[0] * 100,
            test_input[3] / test_input[0] * 100,
            test_input[4] / test_input[0] * 100,
        ],
    )

    assert_series_equal(
        left=_index_to_base_year(test_input),
        right=test_expected,
    )


@parametrize(
    "exception, bucket_name, object_key, file_path, expected",
    [
        (does_not_raise(), "test_bucket", "test_object_key", None, "s3"),
        (does_not_raise(), None, None, "/test/file_path", "local"),
        (raises(ValueError), "test_bucket", None, "/test/file_path", "local"),
        (raises(ValueError), None, "test_object_key", "/test/file_path", "local"),
        (raises(ValueError), "test_bucket", None, None, "s3"),
        (raises(ValueError), None, "test_object_key", None, "s3"),
    ],
)
def test__check_s3_or_local(
    exception: ExceptionInfo,
    bucket_name: Optional[str],
    object_key: Optional[str],
    file_path: Optional[str],
    expected: str,
) -> None:
    """Bucket and key return s3, path returns local, otherwise exception."""
    with exception:
        returned: str = _check_s3_or_local(
            bucket_name=bucket_name,
            object_key=object_key,
            file_path=file_path,
        )

        assert returned == expected
