"""Transformation functions for the Air Seven DataFrame."""

from pandas import DataFrame

from oiflib._helper import _forward_fill_column, _where_column_contains_string


def _forward_fill_measure_column(df: DataFrame) -> DataFrame:
    """Fills the blank cells in measure column with the value from cell above.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`transform_air_seven`.

    Under the hood it calls:

    - :func:`_forward_fill_column`

    Example:
        >>> extracted = extract(
            theme="air",
            indicator="seven",
            )
        >>> extracted_validated = validate(
            theme="air",
            indicator="seven",
            stage="extracted",
            df=extracted,
        )
        >>> _filled = _forward_fill_measure_column(
            df=extracted_validated,
            )

    Args:
        df (DataFrame): The Air Seven DataFrame returned by :func:`extract`.

    Returns:
        DataFrame: A DataFrame with a measure column that doesn't contains
            blanks.
    """
    return _forward_fill_column(
        df=df,
        column_name="Critical level (μg m-3)",
    )


def _filter_years_column(df: DataFrame) -> DataFrame:
    """Removes the summary rows from the filled Air Seven DataFrame.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`transform_air_seven`.

    Under the hood it calls:

    - :func:`_where_column_contains_string`

    Example:
        >>> extracted = extract(
            theme="air",
            indicator="seven",
            )
        >>> extracted_validated = validate(
            theme="air",
            indicator="seven",
            stage="extracted",
            df=extracted,
        )
        >>> _filled = _forward_fill_measure_column(
            df=extracted_validated,
            )
        >>> _filtered = _filter_years_column(
            df=_filled,
            )

    Args:
        df (DataFrame): The DataFrame produced by
            :func:`_forward_fill_measure_column`.

    Returns:
        DataFrame: A DataFrame without the
            ``Change in % area exceeded from 2010 to 2016`` rows.
    """
    return _where_column_contains_string(
        df=df,
        column_name="Concentration data years",
        string="Change",
        does=False,
    )


def _unpivot(df: DataFrame) -> DataFrame:
    """Unpivots the filtered Air Seven DataFrame.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`transform_air_seven`.

    Example:
        >>> extracted = extract(
            theme="air",
            indicator="seven",
            )
        >>> extracted_validated = validate(
            theme="air",
            indicator="seven",
            stage="extracted",
            df=extracted,
        )
        >>> _filled = _forward_fill_measure_column(
            df=extracted_validated,
            )
        >>> _filtered = _filter_years_column(
            df=_filled,
            )
        >>> _unpivoted = _unpivot(
            df=_filtered,
            )

    Args:
        df (DataFrame): The DataFrame produced by :func:`_filter_years_column`.

    Returns:
        DataFrame: The Air Seven data with ``Country`` and
            ``% land area where ammonia concentrations exceed critical levels`` as
            variables.
    """
    return df.melt(
        id_vars=["Concentration data years", "Critical level (μg m-3)"],
        var_name="Country",
        value_name=r"% land area where ammonia concentrations exceed critical levels",
    )


def _filter_country_and_measure_columns(df: DataFrame) -> DataFrame:
    """Filters the unpivoted Air Seven DataFrame.

    .. note:
        This is a private function. It is not intended to be called directly.
        It is called within :func:`transform_air_seven`.

    Under the hood it calls:

    - :func:`_where_column_contains_string`

    Example:
        >>> extracted = extract(
            theme="air",
            indicator="seven",
            )
        >>> extracted_validated = validate(
            theme="air",
            indicator="seven",
            stage="extracted",
            df=extracted,
        )
        >>> _filled = _forward_fill_measure_column(
            df=extracted_validated,
            )
        >>> _filtered = _filter_years_column(
            df=_filled,
            )
        >>> _unpivoted = _unpivot(
            df=_filtered,
            )
        >>> _filtered_again = _filter_country_and_measure_columns(
            df=_unpivoted,
            )

    Args:
        df (DataFrame): The DataFrame produced by :func:`_unpivot`.

    Returns:
        DataFrame: Air Seven data for England and 1 μg m-3 only.
    """
    _df = _where_column_contains_string(
        df=df,
        column_name="Country",
        string="England",
    )

    return _where_column_contains_string(
        df=_df,
        column_name="Critical level (μg m-3)",
        string="1 μg m-3",
    )


def transform_air_seven(df: DataFrame) -> DataFrame:
    """Transforms the extracted Air Seven DataFrame into a tidy format.

    Under the hood, this function calls:

    - :func:`_forward_fill_measure_column` to replace missing variables
    - :func:`_filter_years_column` to remove summary rows
    - :func:`_unpivot` to to convert data into a tidy format
    - :func:`_filter_country_and_measure_columns` to return 1 μg m-3 data for England
      only

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
        >>> transformed = transform_air_seven(
            df=extracted_validated,
            )

    Args:
        df (DataFrame): The extracted Air Seven Dataframe.

    Returns:
        DataFrame: The Air Seven data in a tidy format.
    """
    _filled = _forward_fill_measure_column(
        df=df,
    )

    _filtered = _filter_years_column(
        df=_filled,
    )

    _unpivoted = _unpivot(
        df=_filtered,
    )

    _filtered_again = _filter_country_and_measure_columns(
        df=_unpivoted,
    )

    return _filtered_again
