"""Test functions for OIF modules."""

# third-party packages
from pyspark.sql import DataFrame


def are_dfs_equal(df1: DataFrame, df2: DataFrame) -> bool:
    """Returns true if the schema and data of two dataframes are equal."""
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True
