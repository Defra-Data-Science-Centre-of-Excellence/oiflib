"""Functions for transforming Air Six DataFrames."""

from pandas import DataFrame


def _unpivot(df: DataFrame) -> DataFrame:
    return df.melt(
        id_vars="Year",
        var_name="Country",
        value_name="Percentage habitat area exceeded by deposition data",
    )


def transform_air_six(df: DataFrame) -> DataFrame:
    """Transforms the extracted Air Six DataFrame into a tidy format.

    This function calls the private _unpivot() function that unpivots the extracted
    Air Six DataFrame into a tidy format.

    >>> df = extract(theme="air", indicator="six")
    >>> transform_air_six(df=df)

    Args:
        df (DataFrame): The extracted Air Six Dataframe.

    Returns:
        DataFrame: The Air Six data in a tidy format.
    """
    return _unpivot(df)
