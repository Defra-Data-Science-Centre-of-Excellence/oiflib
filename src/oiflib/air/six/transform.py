"""Functions for transforming Air Six DataFrames."""

from pandas import DataFrame

from oiflib._helper import _where_column_contains_string


def _unpivot(df: DataFrame) -> DataFrame:
    """Returns unpivoted Air Six data.

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

    Args:
        df (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
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

    Under the hood it calls:

    - :func:`_where_column_contains_string`

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
    return _where_column_contains_string(
        df=df,
        column_name="Country",
        string="England",
    )


def transform_air_six(df: DataFrame) -> DataFrame:
    """Transforms the extracted Air Six DataFrame into a tidy format.

    Under the hood, this function calls :func:`_unpivot` to convert the extracted Air
    Six DataFrame into a tidy format and :func:`_filter` to return only the England
    data.

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
        >>> transformed = transform_air_six(
            df=extracted_validated,
            )

    Args:
        df (DataFrame): The extracted Air Six Dataframe.

    Returns:
        DataFrame: The Air Six data in a tidy format.
    """
    return df.pipe(_unpivot).pipe(_filter)
