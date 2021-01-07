"""TODO docstring."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as fn

from oiflib.core import melt


def filter_rows(df: DataFrame) -> DataFrame:
    """TODO docstring."""
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
    """TODO docstring."""
    return df.drop("NFRCode", "SourceName")


def clean_column_values(df: DataFrame) -> DataFrame:
    """TODO docstring."""
    df_cleaned: DataFrame = df.withColumn(
        "ShortPollName", fn.regexp_replace(df.ShortPollName, " Total", "")
    )
    return df_cleaned.withColumn(
        "ShortPollName", fn.regexp_replace(df_cleaned.ShortPollName, "VOC", "NMVOC")
    )


def unpivot(df: DataFrame) -> DataFrame:
    """TODO docstring."""
    return melt(
        df=df,
        id_vars="ShortPollName",
        var_name="Year",
        value_name="Emissions",
    )


def process_air_one(df: DataFrame) -> DataFrame:
    """TODO docstring."""
    df_filtered_rows: DataFrame = filter_rows(df)

    df_dropped_columns: DataFrame = drop_columns(df_filtered_rows)

    df_cleaned_column_values: DataFrame = clean_column_values(df_dropped_columns)

    return unpivot(df_cleaned_column_values)
