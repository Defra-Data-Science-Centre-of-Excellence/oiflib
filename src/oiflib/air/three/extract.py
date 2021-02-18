"""Functions to extract Air Three DataFrames."""
from functools import reduce
from typing import List, Union

from pandas import DataFrame, merge, read_csv

from oiflib.extract import _column_name_to_string


def extract_air_three(
    file_path_base: str,
    range_start: int,
    range_end: int,
    join_on: Union[List[str], str],
    string_to_replace: str = "YYYY",
) -> DataFrame:
    """Extracts and joins CSV files with similar paths into a single wide DataFrame.

    Unlike the other Air indicators, which publish a complete time series every year,
    the Air Three input data is published in yearly chunks. This function uses nested
    list comprehension inside a reduce function too achieve this.

    The first list comprehension statement:

    [
        file_path_base.replace(string_to_replace, str(year))
        for year in range(
            range_start,
            range_end + 1,
        )
    ]

    returns a list of URLs by taking the string supplied by the `file_path_base` arg
    and replacing the string supplied by the `string_to_replace` arg with a range of
    years from the int supplied by `range_start` to the int derived from
    `range_end + 1`.

    The output of that first list comprehension is then used by the second:

    [
        read_csv(
            filepath_or_buffer=file_path,
            skiprows=2,
        )
        for file_path in <output of first list comprehension>
    ]

    which returns a list of DataFrames.

    Finally, the reduce function:

    reduce(
        lambda x, y: merge(left=x, right=y, how="left", on=join),
        <output of second list comprehension>,
    )

    joins them all together. reduce applies a function cumulatively then returns the
    final output (as opposed to accumulate, which returns the intermediate outputs as
    well). So, it joins the first two DataFrames in the list, then joins the third to
    that, and the fourth to that, and so on.

    Args:
        file_path_base (str): A generic version of the file path with an integer element
            to be replaced. e.g. "/path/to/file_YYYY.csv"
        range_start (int): The starting replacement integer. e.g. 1990.
        range_end (int): The final replacement integer. e.g. 2019.
        join_on (Union[List[str], str]): The column names to join on.
        string_to_replace (str): The integer element within the file_path_base to
            replace. Defaults to "YYYY".

    Returns:
        DataFrame: A single wide DataFrame.
    """
    return reduce(
        lambda x, y: merge(left=x, right=y, how="left", on=join_on),
        [
            read_csv(
                filepath_or_buffer=file_path,
                skiprows=2,
            )
            for file_path in [
                file_path_base.replace(string_to_replace, str(year))
                for year in range(
                    range_start,
                    range_end + 1,
                )
            ]
        ],
    ).pipe(_column_name_to_string)
