import pandas as pd

from oiflib.core import column_name_to_string


def extract_air_one() -> pd.DataFrame:
    return pd.read_excel(
        io="http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",
        sheet_name="England API",
        usecols="B:AA",
        skiprows=13,
        nrows=1602,
    ).pipe(column_name_to_string)
