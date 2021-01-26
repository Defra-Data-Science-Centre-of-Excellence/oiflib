"""A dictionary of OIF datasets."""

oif_datasets: dict = {
    "air": {
        "one": {
            "io": "http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",  # noqa
            "sheet_name": "England API",
            "usecols": "B:AA",
            "skiprows": 13,
            "nrows": 1602,
        },
        "two": {
            "io": "https://uk-air.defra.gov.uk/assets/documents/reports/cat09/2006160834_DA_GHGI_1990-2018_v01-04.xlsm",  # noqa
            "sheet_name": "England By Source",
            "usecols": "B:AA",
            "skiprows": 16,
            "nrows": 192,
        },
        "three": {
            "io": "https://uk-air.defra.gov.uk/datastore/pcm/popwmpm252019byUKcountry.csv",  # noqa
            "skiprows": 2,
        },
        "four": {
            "io": "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/881869/O3_tables.ods",  # noqa
            "sheet_name": "Annex1",
            "skiprows": 2,
        },
        "five": {
            "io": "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/881868/NO2_tables.ods",  # noqa
            "sheet_name": "Annex1",
            "skiprows": 2,
        },
        "six": "",
        "seven": "",
    },
}
