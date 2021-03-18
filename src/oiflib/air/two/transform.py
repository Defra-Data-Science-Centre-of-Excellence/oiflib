"""Transformation functions for air_two."""

from pandas import DataFrame

from oiflib._helper import _forward_fill_column


def drop_BaseYear_column(df: DataFrame) -> DataFrame:
    """Drops BaseYear column from air_two DataFrame.

    The BaseYear column is a duplicate of the 1990 column, so it can be dropped
    safely without data lose.

    Args:
        df (DataFrame): A DataFrame with a BaseYear column.

    Returns:
        DataFrame: A DataFrame without a BaseYear column.
    """
    return df.drop(
        columns="BaseYear",
    )


def forward_fill_NCFormat_column(df: DataFrame) -> DataFrame:
    """Fills the blank cells in NCFormat column with the value from cell above.

    Args:
        df (DataFrame): A DataFrame with a NCFormat column that contains blanks.

    Returns:
        DataFrame: A DataFrame with a NCFormat column that doesn't contains blanks.
    """
    return _forward_fill_column(
        df=df,
        column_name="NCFormat",
    )


def unpivot(df: DataFrame) -> DataFrame:
    """Unpivots the air_two DataFrame from wide to long.

    Unpivots the air_two DataFrame from a wide format, where observations are
    identified by the "NCFormat" and "IPCC" variables but the "EmissionYear" variable
    is used as a column header with the "CO2 Equiv" variable as values, to a long
    format, where the "NCFormat", "IPCC", "EmissionYear", and "CO2 Equiv" variables all
    have their own columns.

    Args:
        df (DataFrame): The air_two data in a wide format.

    Returns:
        DataFrame: The air_two data in a long format.
    """
    return df.melt(
        id_vars=["NCFormat", "IPCC"],
        var_name="EmissionYear",
        value_name="CO2 Equiv",
    )


def transform_air_two(df: DataFrame) -> DataFrame:
    """Drop BaseYear, fill NCFormat from above, unpivot <year> names and values.

    This function:

    * Drops the ``BaseYear`` column, as this is a duplicate of the ``1990`` column.
    * Replace NaNs in the NCFormat column with the preceding non-NaN value.
    * Unpivots the <year> column names into an ``EmissionsYear`` column and their values
      into a ``CO2 Equiv`` column.

    Args:
        df (DataFrame): An extarcted and validated Air Two DataFrame.

    Returns:
        DataFrame: A transformed Air Two DataFrame.
    """
    return (
        df.pipe(drop_BaseYear_column).pipe(forward_fill_NCFormat_column).pipe(unpivot)
    )
