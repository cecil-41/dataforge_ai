"""Enrichment joins against the taxi zone lookup dimension table.

See notebooks/03_joins.ipynb for the broadcast-join `.explain()` analysis
and the left_anti check confirming this dataset has zero orphaned
LocationIDs (the join stays `left`/defensive for future data anyway).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Left-join pickup and dropoff LocationIDs to zone/borough names.

    `zones` is aliased twice ("pu"/"do") so the two joins' Zone/Borough
    columns can be unambiguously referenced and renamed. Selecting only the
    needed columns immediately after each join avoids a duplicate
    LocationID collision on the second join.
    """
    pu = zones.alias("pu")
    do = zones.alias("do")
    return (
        trips
        .join(pu, trips.PULocationID == pu.LocationID, "left")
        .select(
            *trips.columns,
            F.col("pu.Zone").alias("pickup_zone"),
            F.col("pu.Borough").alias("pickup_borough"),
        )
        .join(do, trips.DOLocationID == do.LocationID, "left")
        .select(
            *trips.columns,
            "pickup_zone",
            "pickup_borough",
            F.col("do.Zone").alias("dropoff_zone"),
            F.col("do.Borough").alias("dropoff_borough"),
        )
    )
