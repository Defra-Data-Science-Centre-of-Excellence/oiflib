"""Functions for transforming Air Six DataFrames."""

from pandas import DataFrame


def _unpivot(df: DataFrame) -> DataFrame:
    """# TODO summary."""
    return df.melt(
        id_vars="Year",
        var_name="Country",
        value_name="Percentage habitat area exceeded by deposition data",
    )


def _filter(df: DataFrame) -> DataFrame:
    """Returns Air Six data for England only.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`transform_air_six`.

    Example:
        >>> extracted = extract(
            theme="air",
            indicator="six",
            )
        >>> extracted_validated = validate(
            theme="air",
            indicator="six",
            stage="extracted",
            df=extracted,
        )
        >>> _unpivoted = _unpivot(
            df=extracted_validated,
            )
        >>> _filtered = _filter(
            df=_unpivoted,
        )

    Args:
        df (DataFrame): DataFame with data for UK nations.

    Returns:
        DataFrame: DataFame with data for England only.
    """
    return df[df["Country"] == "England"]


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
