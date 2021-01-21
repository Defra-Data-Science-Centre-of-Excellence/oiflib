"""TODO module docstring."""

import pandas as pd


def join_to_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """TODO function docstring.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    df_lookup: pd.DataFrame = pd.read_csv(
        filepath_or_buffer="/home/edfawcetttaylor/repos/oiflib/data/air/two/lookup.csv",
    )

    return pd.merge(
        left=df,
        right=df_lookup,
        how="left",
        on=["NCFormat", "IPCC"],
    )
