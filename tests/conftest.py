# third-party
import pytest


# Define spark context as fixture for use in other tests
@pytest.fixture()
def spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.enableHiveSupport().getOrCreate()
