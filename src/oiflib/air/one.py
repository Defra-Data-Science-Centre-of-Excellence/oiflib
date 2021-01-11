"""TODO docstring."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as fn

from oiflib.core import melt


def filter_rows(df: DataFrame) -> DataFrame:
    """Returns the total rows for the five pollutants.

    Args:
        df (DataFrame): The raw air one input DataFrame.

    Returns:
        DataFrame: A DataFrame including only the total rows for the five pollutants.
    """
    return df.filter(
        df.ShortPollName.isin(
            "NH3 Total",
            "NOx Total",
            "SO2 Total",
            "VOC Total",
            "PM2.5 Total",
        )
    )


def drop_columns(df: DataFrame) -> DataFrame:
    """Removes the unused "NFRCode" and "SourceName" columns.

    Args:
        df (DataFrame): The input DataFrame. It should be the output of filter_rows()
            but doesn't have to be.

    Returns:
        DataFrame: A DataFrame the unused columns removed.
    """
    return df.drop("NFRCode", "SourceName")


def clean_column_values(df: DataFrame) -> DataFrame:
    """Removes " Total" from the "ShortPollName" column and changes "VOC" to "NMVOC".

    Args:
        df (DataFrame): The input DataFrame. It should be the output of drop_columns()
            but doesn't have to be.

    Returns:
        DataFrame: A DataFrame with a cleaned "ShortPollName column.
    """
    df_cleaned: DataFrame = df.withColumn(
        "ShortPollName", fn.regexp_replace(df.ShortPollName, " Total", "")
    )
    return df_cleaned.withColumn(
        "ShortPollName", fn.regexp_replace(df_cleaned.ShortPollName, "VOC", "NMVOC")
    )


def unpivot(df: DataFrame) -> DataFrame:
    """Unpivots the Year column names into a "Year" column.

    Args:
        df (DataFrame): The input DataFrame. It's intended to be the output of
            clean_column_values() but doesn't have to be.

    Returns:
        DataFrame: A long-format DataFrame with "ShortPollName", "Year", and
            "Emissions" columns.
    """
    return melt(
        df=df,
        id_vars="ShortPollName",
        var_name="Year",
        value_name="Emissions",
    )


def process_air_one(df: DataFrame) -> DataFrame:
    """Processes the air one input.

    This function applies the filter_rows(), drop_columns(),
    clean_column_values(), and unpivot() UDFs to the air one
    input data.

    Args:
        df (DataFrame): The raw air one input DataFrame.

    Returns:
        DataFrame: A long-format DataFrame with "ShortPollName", "Year", and
            "Emissions" columns.
    """
    return (
        df.transform(filter_rows)
        .transform(drop_columns)
        .transform(clean_column_values)
        .transform(unpivot)
    )
