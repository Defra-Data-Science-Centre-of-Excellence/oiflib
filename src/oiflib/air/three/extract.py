"""Functions to extract Air Three DataFrames."""

from functools import reduce

import pandas as pd


def extract_air_three() -> pd.DataFrame:
    """TODO function docstring.

    Returns:
        pd.DataFrame: [description]
    """
    return reduce(
        lambda x, y: pd.merge(left=x, right=y, on=["Area code", "Country"]),
        [
            pd.read_csv(
                filepath_or_buffer=url,
                skiprows=2,
            )
            for url in [
                f"https://uk-air.defra.gov.uk/datastore/pcm/popwmpm25{year}byUKcountry.csv"  # noqa: B950 - "line too long" n/a for URL
                for year in range(2011, 2020)
            ]
        ],
    )
