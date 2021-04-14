"""Tests for oiflib core functions."""
from contextlib import nullcontext as does_not_raise

from _pytest._code.code import ExceptionInfo
from pytest import raises
from pytest_cases import parametrize


@parametrize(
    ("module_name, function_name, exception"),
    [
        ("oiflib.extract", "extract", does_not_raise()),
        ("oiflib.format", "format", does_not_raise()),
        ("oiflib.push", "publish", does_not_raise()),
        ("oiflib.validate", "validate", does_not_raise()),
        ("oiflib.invalid_module", "invalid_function", raises(ImportError)),
    ],
    ids=[
        "extract",
        "format",
        "publish",
        "validate",
        "invalid_function",
    ],
)
def test_import_of(
    module_name: str,
    function_name: str,
    exception: ExceptionInfo,
):
    """Submodules are imported into core."""
    with exception:
        module = __import__(module_name, fromlist=[function_name])
        getattr(module, function_name)
