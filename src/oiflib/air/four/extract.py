"""TODO module docstring."""

import pandas as pd


def extract_air_four() -> pd.DataFrame:
    """TODO function docstring.

    Returns:
        pd.DataFrame: [description]
    """
    return pd.read_excel(
        "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/881869/O3_tables.ods",  # noqa: B950
        sheet_name="Annex1",
        skiprows=2,
    )
