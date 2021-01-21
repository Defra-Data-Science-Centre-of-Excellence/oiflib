"""TODO module docstring."""

import pandas as pd


def extract_air_two() -> pd.DataFrame:
    """TODO function docstring.

    Returns:
        pd.DataFrame: [description]
    """
    return pd.read_excel(
        io="https://uk-air.defra.gov.uk/assets/documents/reports/cat09/2006160834_DA_GHGI_1990-2018_v01-04.xlsm",
        sheet_name="England By Source",
        usecols="B:AA",
        skiprows=16,
    )
