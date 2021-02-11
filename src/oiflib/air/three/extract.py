"""Functions to extract Air Three DataFrames."""

from functools import reduce

from pandas import DataFrame, merge, read_csv

from oiflib.extract import _column_name_to_string


def extract_air_three() -> DataFrame:
    """TODO function docstring.

    Returns:
        DataFrame: [description]
    """
    return reduce(
        lambda x, y: merge(left=x, right=y, on=["Area code", "Country"]),
        [
            read_csv(
                filepath_or_buffer=url,
                skiprows=2,
            )
            for url in [
                f"https://uk-air.defra.gov.uk/datastore/pcm/popwmpm25{year}byUKcountry.csv"  # noqa: B950 - "line too long" n/a for URL
                for year in range(2011, 2020)
            ]
        ],
    ).pipe(_column_name_to_string)
