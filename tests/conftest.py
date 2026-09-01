import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A minimal local SparkSession for unit tests.

    local[1] + shuffle.partitions=1: tests use tiny synthetic DataFrames, so
    there's nothing to parallelize -- more partitions would just add
    scheduling overhead. UI disabled since no one's watching localhost:4040
    during a test run.
    """
    session = (
        SparkSession.builder
        .appName("DataForge-Tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
