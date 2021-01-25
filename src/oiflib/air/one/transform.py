"""Functions for transforming the Air One input data into a tidy format using pandas."""

import pandas as pd


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the total rows for the five pollutants.

    Args:
        df (pd.DataFrame): The raw air one input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame including only the total rows for the five pollutants.
    """
    return df.query(
        expr="ShortPollName == ["
        '"NH3 Total",'
        '"NOx Total",'
        '"SO2 Total",'
        '"VOC Total",'
        '"PM2.5 Total"]'
    )


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Removes the unused "NFRCode" and "SourceName" columns.

    Args:
        df (pd.DataFrame): The input DataFrame. It should be the output of filter_rows()
            but doesn't have to be.

    Returns:
        pd.DataFrame: A DataFrame the unused columns removed.
    """
    return df.drop(
        columns=["NFRCode", "SourceName"],
    )


def clean_column_values(df: pd.DataFrame) -> pd.DataFrame:
    """Removes " Total" from the "ShortPollName" column and changes "VOC" to "NMVOC".

    Args:
        df (pd.DataFrame): The input DataFrame. It should be the output of
            drop_columns() but doesn't have to be.

    Returns:
        pd.DataFrame: A DataFrame with a cleaned "ShortPollName column.
    """
    return df.assign(
        ShortPollName=(
            df.ShortPollName.str.replace(pat=" Total", repl="").str.replace(
                pat="VOC",
                repl="NMVOC",
            )
        )
    )


def unpivot(df: pd.DataFrame) -> pd.DataFrame:
    """Unpivots the Year column names into a "Year" column.

    Args:
        df (pd.DataFrame): The input DataFrame. It's intended to be the output of
            clean_column_values() but doesn't have to be.

    Returns:
        pd.DataFrame: A long-format DataFrame with "ShortPollName", "Year", and
            "Emissions" columns.
    """
    return df.melt(
        id_vars="ShortPollName",
        var_name="EmissionYear",
        value_name="Emission",
    )


def transform_air_one(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the air one input.

    This function applies the filter_rows(), drop_columns(),
    clean_column_values(), and unpivot() UDFs to the air one
    input data.

    Args:
        df (pd.DataFrame): The raw air one input DataFrame.

    Returns:
        pd.DataFrame: A long-format DataFrame with "ShortPollName", "EmissionYear", and
            "Emission" columns.
    """
    return (
        df.pipe(filter_rows).pipe(drop_columns).pipe(clean_column_values).pipe(unpivot)
    )
