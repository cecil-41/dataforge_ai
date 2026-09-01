"""Raw data ingestion: schema-drift-safe reading of the NYC TLC parquet files.

See notebooks/01_fundamentals.ipynb for the four failed approaches that
motivated the read-as-is-then-cast pattern below (the monthly files disagree
on column types in both directions, plus a name-casing clash on
airport_fee/Airport_fee).
"""
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TARGET_TYPES = {
    "VendorID": "long", "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp", "passenger_count": "double",
    "trip_distance": "double", "RatecodeID": "double",
    "store_and_fwd_flag": "string", "PULocationID": "long",
    "DOLocationID": "long", "payment_type": "long", "fare_amount": "double",
    "extra": "double", "mta_tax": "double", "tip_amount": "double",
    "tolls_amount": "double", "improvement_surcharge": "double",
    "total_amount": "double", "congestion_surcharge": "double",
    "airport_fee": "double",
}


def read_and_cast(spark: SparkSession, path: str) -> DataFrame:
    """Read one monthly Parquet file and cast every column to TARGET_TYPES.

    Casting values in-engine (instead of forcing the reader's physical
    schema via a declared StructType) is what lets files with genuine
    schema drift be unioned safely -- the vectorized Parquet reader refuses
    unsafe physical conversions (e.g. INT64 -> double), but `.cast()` on
    already-read values works fine.
    """
    df = spark.read.parquet(path)
    df = df.toDF(*[c.lower() for c in df.columns])
    for col, target in TARGET_TYPES.items():
        df = df.withColumn(col, F.col(col.lower()).cast(target))
    return df.select(*TARGET_TYPES.keys())


def read_trips(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read + cast multiple monthly files and union them into one DataFrame."""
    return reduce(DataFrame.unionByName, [read_and_cast(spark, p) for p in paths])
