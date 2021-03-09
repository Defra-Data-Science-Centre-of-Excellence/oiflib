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
