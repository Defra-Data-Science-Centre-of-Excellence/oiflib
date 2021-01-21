"""TODO module docstring."""

import pandas as pd


def extract_air_one() -> pd.DataFrame:
    """TODO function docstring.

    Returns:
        pd.DataFrame: [description]
    """
    return pd.read_excel(
        io="http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",
        sheet_name="England API",
        usecols="B:AA",
        skiprows=13,
    )
