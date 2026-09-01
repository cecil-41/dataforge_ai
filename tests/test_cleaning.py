from dataforge_ai.cleaning import clean_trips

# Columns matching the raw (cast) schema that clean_trips expects.
COLUMNS = [
    "case", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount", "total_amount",
    "PULocationID", "DOLocationID",
]

ROWS = [
    # Valid trip -- should survive untouched.
    ("keep_valid", "2023-01-15 08:00:00", "2023-01-15 08:10:00", 1.0, 2.0, 10.0, 12.0, 1, 2),
    # Same dedup key as keep_valid (pickup/dropoff time, PU/DO, distance,
    # fare) but a different total_amount -- total_amount isn't part of the
    # dedup key, so this is a genuine duplicate and one twin must be dropped.
    ("dup_of_valid", "2023-01-15 08:00:00", "2023-01-15 08:10:00", 1.0, 2.0, 10.0, 99.0, 1, 2),
    # Rule 1: negative fare/total -> drop.
    ("drop_negative_fare", "2023-01-15 09:00:00", "2023-01-15 09:05:00", 1.0, 3.0, -5.0, -5.0, 3, 4),
    # Rule 2: dropoff before pickup -> drop.
    ("drop_dropoff_before_pickup", "2023-01-15 09:00:00", "2023-01-15 08:59:00", 1.0, 3.0, 10.0, 12.0, 3, 4),
    # Rule 3: pickup date outside Jan-Mar 2023 -> drop.
    ("drop_out_of_range_date", "2022-12-31 23:00:00", "2022-12-31 23:10:00", 1.0, 3.0, 10.0, 12.0, 3, 4),
    # Rule 4: trip_distance > 100 miles -> drop.
    ("drop_too_far", "2023-01-15 09:00:00", "2023-01-15 09:30:00", 1.0, 150.0, 10.0, 12.0, 3, 4),
    # Rule 5: zero distance AND zero fare -> drop (no signal this is real).
    ("drop_zero_distance_zero_fare", "2023-01-15 09:00:00", "2023-01-15 09:05:00", 1.0, 0.0, 0.0, 0.0, 3, 4),
    # Rule 5 counter-case: zero distance but a real fare charged -> keep.
    ("keep_zero_distance_paid", "2023-01-15 09:00:00", "2023-01-15 09:05:00", 1.0, 0.0, 5.0, 5.0, 3, 4),
    # Rule 1 (passenger fill): null passenger_count -> filled with 1, kept.
    ("keep_null_passenger_filled", "2023-01-15 10:00:00", "2023-01-15 10:05:00", None, 2.0, 8.0, 9.0, 5, 6),
]


def test_clean_trips_applies_all_rules(spark):
    df = spark.createDataFrame(ROWS, COLUMNS)
    result = clean_trips(df)

    survivors = {row["case"] for row in result.select("case").collect()}

    assert result.count() == 3
    assert "drop_negative_fare" not in survivors
    assert "drop_dropoff_before_pickup" not in survivors
    assert "drop_out_of_range_date" not in survivors
    assert "drop_too_far" not in survivors
    assert "drop_zero_distance_zero_fare" not in survivors
    assert "keep_zero_distance_paid" in survivors
    assert "keep_null_passenger_filled" in survivors
    # Exactly one of the two duplicate-key rows should survive dedup.
    assert len(survivors & {"keep_valid", "dup_of_valid"}) == 1


def test_clean_trips_fills_null_passenger_count_with_mode(spark):
    df = spark.createDataFrame(ROWS, COLUMNS)
    result = clean_trips(df)

    null_row = result.filter(result["case"] == "keep_null_passenger_filled").first()
    assert null_row["passenger_count"] == 1.0
