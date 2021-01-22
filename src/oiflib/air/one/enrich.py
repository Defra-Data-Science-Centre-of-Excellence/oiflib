import pandas as pd

from oiflib.core import index_to_base_year


def index_emissions_to_base_year(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Index=(df.groupby("ShortPollName").Emissions.apply(index_to_base_year))
    )


def enrich_air_one(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(index_emissions_to_base_year)
