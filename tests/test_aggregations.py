from dataforge_ai.aggregations import hourly_demand, rank_zones_by_borough

HOURLY_COLUMNS = ["pickup_borough", "pickup_hour", "fare_amount"]
HOURLY_ROWS = [
    ("Manhattan", 8, 10.0),
    ("Manhattan", 8, 12.0),
    ("Manhattan", 9, 20.0),
    ("Brooklyn", 8, 5.0),
]

ZONE_COLUMNS = ["pickup_borough", "pickup_zone"]
ZONE_ROWS = [
    ("Manhattan", "A"),
    ("Manhattan", "A"),
    ("Manhattan", "B"),
    ("Brooklyn", "C"),
]


def test_hourly_demand_ranks_and_accumulates_within_borough(spark):
    df = spark.createDataFrame(HOURLY_ROWS, HOURLY_COLUMNS)
    result = {
        (row["pickup_borough"], row["pickup_hour"]): row
        for row in hourly_demand(df).collect()
    }

    manhattan_8 = result[("Manhattan", 8)]
    manhattan_9 = result[("Manhattan", 9)]
    brooklyn_8 = result[("Brooklyn", 8)]

    assert manhattan_8["trip_count"] == 2
    assert manhattan_8["avg_fare"] == 11.0
    assert manhattan_8["demand_rank"] == 1  # busiest hour in Manhattan
    assert manhattan_8["cumulative_trips"] == 2

    assert manhattan_9["trip_count"] == 1
    assert manhattan_9["demand_rank"] == 2
    assert manhattan_9["cumulative_trips"] == 3  # 2 (hour 8) + 1 (hour 9)

    # Ranks restart per borough -- Brooklyn's only hour is still rank 1.
    assert brooklyn_8["demand_rank"] == 1
    assert brooklyn_8["cumulative_trips"] == 1


def test_rank_zones_by_borough_restarts_rank_per_partition(spark):
    df = spark.createDataFrame(ZONE_ROWS, ZONE_COLUMNS)
    result = {
        (row["pickup_borough"], row["pickup_zone"]): row
        for row in rank_zones_by_borough(df).collect()
    }

    assert result[("Manhattan", "A")]["trip_count"] == 2
    assert result[("Manhattan", "A")]["borough_rank"] == 1
    assert result[("Manhattan", "B")]["borough_rank"] == 2
    assert result[("Brooklyn", "C")]["borough_rank"] == 1
