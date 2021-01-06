"""Core functions used across all OIF modules."""

# standard library
from typing import List

# third-party
from pyspark.sql import DataFrame


def melt(
    df: DataFrame, id_vars: str, var_name: str = "variable", value_name: str = "value"
) -> DataFrame:
    """Unpivots a DataFrame from wide to long format.

    Args:
        df: A Spark DataFrame.
        id_vars: Column(s) to use as identifier variables.
        var_name: Name to use for the variable column. Defaults to 'variable'.
        value_name: Name to use for the value column. Defaults to 'value'.

    Returns:
        DataFrame: An unpivoted Spark DataFrame.

    Raises:
        Not yet implemented.
    """
    var_columns: List[str] = [col for col in df.columns if col not in id_vars]
    expression: str = ", ".join(
        [", ".join(["'" + x + "'", "`" + x + "`"]) for x in var_columns]
    )
    return df.selectExpr(
        id_vars, f"stack({len(var_columns)},{expression}) as ({var_name}, {value_name})"
    ).orderBy(var_name, id_vars)
