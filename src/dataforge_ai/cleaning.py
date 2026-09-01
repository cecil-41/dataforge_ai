"""ETL cleaning rules for raw NYC taxi trip data.

See notebooks/02_cleaning_and_etl.ipynb for the audited per-rule row-removal
reconciliation (82,689 of 9,384,487 rows removed, 0.88%) and the RatecodeID
analysis behind the zero-distance-but-paid rule.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_trips(df: DataFrame) -> DataFrame:
    """Apply the Phase-2 data-quality rules to raw (cast) trip data.

    In order: drop negative fare/total, drop dropoff-before-pickup, drop
    pickups outside the labelled Jan-Mar 2023 range, drop unrealistic trip
    distances (>100 miles), drop zero-distance trips with no fare charged,
    fill null/zero passenger counts with the mode (1), and drop exact
    duplicate trips (same pickup/dropoff time, locations, distance, fare).
    """
    return (
        df
        .filter((F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0))
        .filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        .filter(
            (F.col("tpep_pickup_datetime") >= F.lit("2023-01-01")) &
            (F.col("tpep_pickup_datetime") < F.lit("2023-04-01"))
        )
        .filter(F.col("trip_distance") <= 100)
        .filter((F.col("trip_distance") > 0) | (F.col("fare_amount") > 0))
        .withColumn(
            "passenger_count",
            F.when(
                F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
                F.lit(1),
            ).otherwise(F.col("passenger_count")),
        )
        .dropDuplicates([
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
        ])
    )
