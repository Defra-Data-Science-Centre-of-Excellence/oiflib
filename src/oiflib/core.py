"""Core functions used across all OIF modules."""

# standard library
from typing import List

# third-party
from pyspark.sql import DataFrame


def melt(
    df: DataFrame, id_vars: str, var_name: str = "variable", value_name: str = "value"
) -> DataFrame:
    """"Unpivot a DataFrame from wide to long format"""
    var_columns: List[str] = [col for col in df.columns if col not in id_vars]
    expression: str = ", ".join(
        [", ".join(["'" + x + "'", "`" + x + "`"]) for x in var_columns]
    )
    return df.selectExpr(
        id_vars, f"stack({len(var_columns)},{expression}) as ({var_name}, {value_name})"
    ).orderBy(var_name, id_vars)
