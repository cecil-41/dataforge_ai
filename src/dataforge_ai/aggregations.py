"""Aggregation and window-function metrics over the zone-enriched trips table.

See notebooks/04_aggregations_and_windows.ipynb for the worked examples and
hands-on task these were extracted from.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def rank_zones_by_borough(trips: DataFrame) -> DataFrame:
    """Trip count per pickup zone, ranked (1 = busiest) within its borough."""
    zone_counts = (
        trips
        .groupBy("pickup_borough", "pickup_zone")
        .agg(F.count("*").alias("trip_count"))
    )
    rank_window = Window.partitionBy("pickup_borough").orderBy(F.desc("trip_count"))
    return zone_counts.withColumn("borough_rank", F.rank().over(rank_window))


def hourly_demand(trips: DataFrame) -> DataFrame:
    """Per-borough, per-hour trip_count/avg_fare, demand rank, and a running
    total of trips across the day (hour 0-23).

    Expects `trips` to already have a `pickup_hour` column (e.g. via
    `F.hour("tpep_pickup_datetime")`) -- kept out of this function so it
    stays focused on the aggregation, not datetime extraction.
    """
    hourly = (
        trips
        .groupBy("pickup_borough", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )
    rank_window = Window.partitionBy("pickup_borough").orderBy(F.desc("trip_count"))
    running_window = Window.partitionBy("pickup_borough").orderBy("pickup_hour")
    return (
        hourly
        .withColumn("demand_rank", F.dense_rank().over(rank_window))
        .withColumn("cumulative_trips", F.sum("trip_count").over(running_window))
        .orderBy("pickup_borough", "pickup_hour")
    )
