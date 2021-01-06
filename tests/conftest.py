import pytest


@pytest.fixture()
def spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.enableHiveSupport().getOrCreate()
