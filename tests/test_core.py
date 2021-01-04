from oiflib.core import are_dfs_equal
from pyspark.sql import SparkSession

## Create spark session
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

## Create test data
input_left = spark.createDataFrame(data=[[1]], schema=["A"])

input_right_true = input_left

input_right_false_schema = spark.createDataFrame(data=[[1]], schema=["B"])

input_right_false_collect = spark.createDataFrame(data=[[2]], schema=["A"])

input_right_false_type = spark.createDataFrame(data=[["1"]], schema=["A"])

## Define tests
def test_are_dfs_equal_true():
    """Test that calling are_dfs_equal on two identical dataframes returns true"""
    assert are_dfs_equal(input_left, input_right_true) == True


def test_are_dfs_equal_false_schema():
    """Test that calling are_dfs_equal on dataframes with different schemas returns false"""
    assert are_dfs_equal(input_left, input_right_false_schema) == False


def test_are_dfs_equal_false_collect():
    """Test that calling are_dfs_equal on dataframes with different data values returns false"""
    assert are_dfs_equal(input_left, input_right_false_collect) == False


def test_are_dfs_equal_false_type():
    """Test that calling are_dfs_equal on dataframes with identical data values but different data types returns false"""
    assert are_dfs_equal(input_left, input_right_false_type) == False
