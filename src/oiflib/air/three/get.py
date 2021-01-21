import pandas as pd

df_a3_melted = pd.concat(
    [
        (
            pd.read_csv(filepath_or_buffer=url, skiprows=2,).melt(
                id_vars=["Area code", "Country"],
                value_name="ugm-3",
            )
        )
        for url in [
            f"https://uk-air.defra.gov.uk/datastore/pcm/popwmpm25{year}byUKcountry.csv"
            for year in range(2011, 2020)
        ]
    ]
)
