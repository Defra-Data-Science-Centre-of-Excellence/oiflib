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
        "air_one": "1-1-1",
        "air_two": "1-2-1",
        "air_three": "1-3-1",
        "air_four": "1-4-1",
        "air_five": "1-5-1",
        "air_six": "1-6-1",
        "air_seven": "1-7-1",
        "water_one": "2-2-1",
        "water_two": "2-2-1",
        "water_three": "2-3-1",
        "water_four": "2-4-1",
        "water_five": "2-5-1",
        "water_six": "2-6-1",
        "water_seven": "2-7-1",
        "seas_and_estuaries_one": "3-1-1",
        "seas_and_estuaries_two": "3-2-1",
        "seas_and_estuaries_three": "3-3-1",
        "seas_and_estuaries_four": "3-4-1",
        "seas_and_estuaries_five": "3-5-1",
        "seas_and_estuaries_six": "3-6-1",
        "seas_and_estuaries_seven": "3-7-1",
        "seas_and_estuaries_eight": "3-8-1",
        "seas_and_estuaries_nine": "3-9-1",
        "seas_and_estuaries_ten": "3-10-1",
        "seas_and_estuaries_eleven": "3-11-1",
        "wildlife_one": "4-1-1",
        "wildlife_two": "4-2-1",
        "wildlife_three": "4-3-1",
        "wildlife_four": "4-4-1",
        "wildlife_five": "4-5-1",
        "wildlife_six": "4-6-1",
        "wildlife_seven": "4-7-1",
        "natural_resources_one": "5-1-1",
        "natural_resources_two": "5-2-1",
        "natural_resources_three": "5-3-1",
        "natural_resources_four": "5-4-1",
        "natural_resources_five": "5-5-1",
        "natural_resources_six": "5-6-1",
        "natural_resources_seven": "5-7-1",
        "natural_resources_eight": "5-8-1",
        "natural_resources_nine": "5-9-1",
        "resilience_one": "6-1-1",
        "resilience_two": "6-2-1",
        "resilience_three": "6-3-1",
        "natural_beauty_and_engagement_one": "7-1-1",
        "natural_beauty_and_engagement_two": "7-2-1",
        "natural_beauty_and_engagement_three": "7-3-1",
        "natural_beauty_and_engagement_four": "7-4-1",
        "natural_beauty_and_engagement_five": "7-5-1",
        "natural_beauty_and_engagement_six": "7-6-1",
        "natural_beauty_and_engagement_seven": "7-7-1",
        "biosecurity_chemical_and_noise_one": "8-1-1",
        "biosecurity_chemical_and_noise_two": "8-2-1",
        "biosecurity_chemical_and_noise_three": "8-3-1",
        "biosecurity_chemical_and_noise_four": "8-4-1",
        "biosecurity_chemical_and_noise_five": "8-5-1",
        "resource_use_and_waste_one": "9-1-1",
        "resource_use_and_waste_two": "9-2-1",
        "resource_use_and_waste_three": "9-3-1",
        "resource_use_and_waste_four": "9-4-1",
        "resource_use_and_waste_five": "9-5-1",
        "resource_use_and_waste_six": "9-6-1",
        "international_one": "10-1-1",
        "international_two": "10-2-1",
        "international_three": "10-3-1",
        "international_four": "10-4-1",
    },
)
