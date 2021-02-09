"""Core functions for oiflib."""

from pandas import Series

from oiflib.extract import extract  # noqa: F401
from oiflib.schema import dict_schema  # noqa: F401
from oiflib.validate import validate  # noqa: F401


def index_to_base_year(series: Series) -> Series:
    """Divide each value in a series by the first value, then multiply it by 100.

    Args:
        series (Series): A pandas Series to index. The Function assumes this Series is
            ordered.

    Returns:
        Series: A Series of equal length, containing indexed values.
    """
    return series.div(series.iloc[0]).mul(100)
