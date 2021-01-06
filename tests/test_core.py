from oiflib.testing import are_dfs_equal
from oiflib.core import melt
from pyspark.sql import SparkSession, Row

## Create spark session
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

## Create test data
input = spark.createDataFrame(
    data=[
        ["a", 1, 2],
        ["b", 3, 4],
        ["c", 5, 6],
    ],
    schema=["A", "B", "C"],
)

output_expected = spark.createDataFrame(
    data=[
        ["a", "B", 1],
        ["b", "B", 3],
        ["c", "B", 5],
        ["a", "C", 2],
        ["b", "C", 4],
        ["c", "C", 6],
    ],
    schema=["A", "variable", "value"],
)

## Apply function to input
output_received = melt(df=input, id_vars="A")

## Define tests
def test_received_equals_expected():
    """Return true if received DataFrame and expected DataFrame are equal"""
    assert are_dfs_equal(output_expected, output_received) == True
