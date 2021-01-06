import pytest
from oiflib.testing import are_dfs_equal


@pytest.fixture
def input_left(spark):
    return spark.createDataFrame(data=[[1]], schema=["A"])


def test_are_dfs_equal_true(input_left):
    """Test that calling are_dfs_equal on two identical dataframes returns true"""
    input_right = input_left
    assert are_dfs_equal(input_left, input_right)


def test_are_dfs_equal_false_schema(spark, input_left):
    """Test that calling are_dfs_equal on dataframes with different schemas returns false"""
    input_right = spark.createDataFrame(data=[[1]], schema=["B"])
    assert are_dfs_equal(input_left, input_right) is False


def test_are_dfs_equal_false_collect(spark, input_left):
    """Test that calling are_dfs_equal on dataframes with different data values returns false"""
    input_right = spark.createDataFrame(data=[[2]], schema=["A"])
    assert are_dfs_equal(input_left, input_right) is False


def test_are_dfs_equal_false_type(spark, input_left):
    """Test that calling are_dfs_equal on dataframes with identical data values but different data types returns false"""
    input_right = spark.createDataFrame(data=[["1"]], schema=["A"])
    assert are_dfs_equal(input_left, input_right) is False
