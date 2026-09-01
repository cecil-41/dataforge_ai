from dataforge_ai.joins import join_zones

ZONE_COLUMNS = ["LocationID", "Borough", "Zone"]
ZONES = [
    (1, "Manhattan", "Zone A"),
    (2, "Brooklyn", "Zone B"),
    (3, "Queens", "Zone C"),
]

TRIP_COLUMNS = ["trip_id", "PULocationID", "DOLocationID"]
TRIPS = [
    ("t1", 1, 2),   # both sides match
    ("t2", 3, 3),   # both sides match, same zone
    ("t3", 99, 1),  # PU has no match in zones -> left join keeps the row, nulls the zone
]


def test_join_zones_enriches_matched_rows(spark):
    trips = spark.createDataFrame(TRIPS, TRIP_COLUMNS)
    zones = spark.createDataFrame(ZONES, ZONE_COLUMNS)

    result = join_zones(trips, zones).collect()
    by_id = {row["trip_id"]: row for row in result}

    assert len(result) == 3
    assert by_id["t1"]["pickup_zone"] == "Zone A"
    assert by_id["t1"]["pickup_borough"] == "Manhattan"
    assert by_id["t1"]["dropoff_zone"] == "Zone B"
    assert by_id["t1"]["dropoff_borough"] == "Brooklyn"


def test_join_zones_left_join_preserves_unmatched_rows(spark):
    trips = spark.createDataFrame(TRIPS, TRIP_COLUMNS)
    zones = spark.createDataFrame(ZONES, ZONE_COLUMNS)

    result = join_zones(trips, zones).collect()
    by_id = {row["trip_id"]: row for row in result}

    # t3's PULocationID=99 has no match in zones -- an inner join would have
    # dropped this row entirely; left preserves it with null pickup fields.
    assert by_id["t3"]["pickup_zone"] is None
    assert by_id["t3"]["pickup_borough"] is None
    assert by_id["t3"]["dropoff_zone"] == "Zone A"
