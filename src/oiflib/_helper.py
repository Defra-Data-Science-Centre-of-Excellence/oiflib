from typing import Dict, Optional

from pandas import Series

theme_lookup: Dict[str, str] = {
    "air": "1",
}

indicator_lookup: Dict[str, str] = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
}


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
    """# TODO [summary].

    Args:
        bucket_name (str): # TODO [description].
        object_key (str): # TODO [description].
        file_path (str): # TODO [description].

    Raises:
        ValueError: # TODO [description]

    Returns:
        str: # TODO [description]
    """
    if bucket_name and object_key and not file_path:
        return "s3"
    elif not bucket_name and not object_key and file_path:
        return "local"
    else:
        raise ValueError(
            "You must supply either bucket_name and object_key to read from s3 or path \
            to read from a local file"
        )
