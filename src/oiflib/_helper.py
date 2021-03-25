"""Private helper functions."""
from typing import Optional

from frozendict import frozendict
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


_oiflib_to_sdg_lookup: frozendict = frozendict(
    {
        "air_one": "1-1-1",
        "air_two": "7-1-1",
        "air_three": "1-1-2",
        "air_four": "1-1-3",
        "air_five": "1-1-4",
        "air_six": "1-1-5",
        "air_seven": "1-1-6",
        "water_one": "2-2-1",
        "water_two": "2-0-1",
        "water_three": "2-2-2",
        "water_four": "2-4-1",
        "water_five": "2-0-2",
        "water_six": "2-0-3",
        "water_seven": "3-7-1",
        "seas_and_estuaries_one": "8-5-1",
        "seas_and_estuaries_two": "3-4-1",
        "seas_and_estuaries_three": "3-1-1",
        "seas_and_estuaries_four": "3-1-2",
        "seas_and_estuaries_five": "3-1-3",
        "seas_and_estuaries_six": "3-1-4",
        "seas_and_estuaries_seven": "3-3-1",
        "seas_and_estuaries_eight": "3-3-2",
        "seas_and_estuaries_nine": "3-4-5",
        "seas_and_estuaries_ten": "5-4-3",
        "seas_and_estuaries_eleven": "5-4-4",
        "wildlife_one": "3-6-1",
        "wildlife_two": "3-2-1",
        "wildlife_three": "5-0-1",
        "wildlife_four": "3-7-2",
        "wildlife_five": "3-7-3",
        "wildlife_six": "3-7-4",
        "wildlife_seven": "3-7-5",
        "natural_resources_one": "5-5-1",
        "natural_resources_two": "5-5-2",
        "natural_resources_three": "5-5-3",
        "natural_resources_four": "5-5-4",
        "natural_resources_five": "5-3-1",
        "natural_resources_six": "5-3-2",
        "natural_resources_seven": "5-2-1",
        "natural_resources_eight": "5-0-2",
        "natural_resources_nine": "5-4-5",
        "resilience_one": "4-5-1",
        "resilience_two": "4-5-2",
        "resilience_three": "4-4-1",
        "natural_beauty_and_engagement_one": "6-1-1",
        "natural_beauty_and_engagement_two": "6-1-2",
        "natural_beauty_and_engagement_three": "6-2-1",
        "natural_beauty_and_engagement_four": "6-2-2",
        "natural_beauty_and_engagement_five": "6-3-1",
        "natural_beauty_and_engagement_six": "6-3-2",
        "natural_beauty_and_engagement_seven": "6-2-3",
        "biosecurity_chemical_and_noise_one": "10-1-1",
        "biosecurity_chemical_and_noise_two": "10-1-2",
        "biosecurity_chemical_and_noise_three": "9-2-1",
        "biosecurity_chemical_and_noise_four": "9-0-1",
        "biosecurity_chemical_and_noise_five": "6-0-2",
        "resource_use_and_waste_one": "8-1-1",
        "resource_use_and_waste_two": "5-1-1",
        "resource_use_and_waste_three": "8-1-2",
        "resource_use_and_waste_four": "8-1-3",
        "resource_use_and_waste_five": "9-4-1",
        "resource_use_and_waste_six": "8-4-1",
        "international_one": "0-0-1",
        "international_two": "0-0-2",
        "international_three": "0-0-3",
        "international_four": "0-0-4",
    },
)
