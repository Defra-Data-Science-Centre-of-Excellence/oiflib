"""Private helper functions."""
from types import MappingProxyType
from typing import Optional

from pandas import Series


def _index_to_base_year(series: Series) -> Series:
    """Divide each value in a series by the first value, then multiply it by 100.

    Args:
        series (Series): A pandas Series to index. The Function assumes this Series is
            ordered.

    Returns:
        Series: A Series of equal length, containing indexed values.
    """
    return series.div(series.iloc[0]).mul(100)


def _check_s3_or_local(
    bucket_name: Optional[str],
    object_key: Optional[str],
    file_path: Optional[str],
) -> str:
    """Returns "s3", "local", or `ValueError` based on combination of `kwargs` provided.

    A private function called within extract and validate. Not intended to be called
    directly.

    Examples:
        Returns "s3" if `bucket_name` and `object_key` are provided and `file_path` is
        `None`. For example:

        >>> _check_s3_or_local(
            bucket_name="bucket name",
            object_key="object key",
            file_path=None,
        )
        "s3"

        Returns "local" if `file_path` is provided and `bucket_name` and `object_key`
        are `None`. For example:

        >>> _check_s3_or_local(
            bucket_name=None,
            object_key=None,
            file_path="/path/to/file",
        )
        "local"

        Raises a ValueError for any other combination of `kwargs`. For example:

        >>> _check_s3_or_local(
            bucket_name="bucket name",
            object_key=None,
            file_path="/path/to/file",
        )
        ValueError(
            "You must supply either bucket_name and object_key to read from s3 or path "
            "to read from a local file"
        )

    Args:
        bucket_name (str): s3 bucket name.
        object_key (str): s3 object key.
        file_path (str): Path to file.

    Raises:
        ValueError: If an invalid combination of kwargs is provided.

    Returns:
        str: Either "s3" or "local".
    """
    if bucket_name and object_key and not file_path:
        return "s3"
    elif not bucket_name and not object_key and file_path:
        return "local"
    else:
        raise ValueError(
            "You must supply either bucket_name and object_key to read from s3 or path "
            "to read from a local file"
        )


_oiflib_to_sdg_lookup: MappingProxyType = MappingProxyType(
    {
        "A1": "1-1-1",
        "A2": "1-2-1",
        "A3": "1-3-1",
        "A4": "1-4-1",
        "A5": "1-5-1",
        "A6": "1-6-1",
        "A7": "1-7-1",
        "B1a": "2-1-1",
        "B1b": "2-1-2",
        "B2": "2-2-1",
        "B3a": "2-3-1",
        "B3b": "2-3-2",
        "B3c": "2-3-3",
        "B4": "2-4-1",
        "B5": "2-5-1",
        "B6": "2-6-1",
        "B7a": "2-7-1",
        "B7b": "2-7-2",
        "C1a": "3-1-1",
        "C1b": "3-1-2",
        "C1c": "3-1-3",
        "C2ai": "3-2-1-a",
        "C2aii": "3-2-1-b",
        "C3ai": "3-3-1-a",
        "C3aii": "3-3-1-b",
        "C3b": "3-3-2",
        "C4": "3-4-1",
        "C5": "3-5-1",
        "C6": "3-6-1",
        "C7a": "3-7-1",
        "C7b": "3-7-2",
        "C8": "3-8-1",
        "C9": "3-9-1",
        "C10a": "3-10-1",
        "C10b": "3-10-2",
        "C11": "3-11-1",
        "D1": "4-1-1",
        "D2a": "4-2-1",
        "D2b": "4-2-2",
        "D3": "4-3-1",
        "D4": "4-4-1",
        "D5": "4-5-1",
        "D6ai": "4-6-1-a",
        "D6aii": "4-6-1-b",
        "D6bi": "4-6-2-a",
        "D6bii": "4-6-2-b",
        "D7i": "4-7-1-a",
        "D7ii": "4-7-1-b",
        "E1": "5-1-1",
        "E2": "5-2-1",
        "E3": "5-3-1",
        "E4": "5-4-1",
        "E5": "5-5-1",
        "E6": "5-6-1",
        "E7": "5-7-1",
        "E8a": "5-8-1",
        "E8b": "5-8-2",
        "E9": "5-9-1",
        "F1": "6-1-1",
        "F2": "6-2-1",
        "F3": "6-3-1",
        "G1": "7-1-1",
        "G2a": "7-2-1",
        "G2b": "7-2-2",
        "G3": "7-3-1",
        "G4": "7-4-1",
        "G5": "7-5-1",
        "G6": "7-6-1",
        "G7": "7-7-1",
        "H1": "8-1-1",
        "H2": "8-2-1",
        "H3a": "8-3-1",
        "H3b": "8-3-2",
        "H4": "8-4-1",
        "H5": "8-5-1",
        "J1": "9-1-1",
        "J2a": "9-2-1",
        "J2b": "9-2-2",
        "J3": "9-3-1",
        "J4": "9-4-1",
        "J5": "9-5-1",
        "J6a": "9-6-1",
        "J6b": "9-6-2",
        "K1": "10-1-1",
        "K2": "10-2-1",
        "K3": "10-3-1",
        "K4": "10-4-1",
    },
)
