# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipelines (DLT): medallion, declaratively
# MAGIC
# MAGIC The same Bronze -> Silver -> Gold logic as
# MAGIC [07a_bronze.py](../notebooks/07a_bronze.py) /
# MAGIC [07b_silver.py](../notebooks/07b_silver.py) /
# MAGIC [07c_gold.py](../notebooks/07c_gold.py), rebuilt as a
# MAGIC [Lakeflow Declarative Pipeline](https://learn.microsoft.com/en-us/azure/databricks/dlt/)
# MAGIC (formerly DLT) instead of a hand-wired Job.
# MAGIC
# MAGIC **Job vs. DLT, the actual difference this notebook demonstrates:**
# MAGIC - In the Job (07a/b/c), *we* wrote the dependency wiring ("Depends on"
# MAGIC   in the UI) and *we* wrote the row-filtering logic as plain
# MAGIC   `.filter()` calls with no visibility into how many rows each rule
# MAGIC   drops.
# MAGIC - Here, `@dlt.table` functions declare *what* each table is, and the
# MAGIC   pipeline engine infers the Bronze -> Silver -> Gold dependency graph
# MAGIC   automatically from which tables read which. `@dlt.expect_or_drop`
# MAGIC   turns `clean_trips`'s filtering rules into named, independently
# MAGIC   tracked data-quality constraints — the pipeline UI shows exactly how
# MAGIC   many rows each rule dropped, per run.
# MAGIC
# MAGIC **Why the logic is inlined instead of importing `dataforge_ai`:** DLT
# MAGIC pipeline notebooks run in a managed execution model that doesn't support
# MAGIC `dbutils.library.restartPython()` (used in 07a/b/c and 09 to make an
# MAGIC editable package install visible) — so rather than fighting that
# MAGIC constraint, the same transformation rules from `io.py`/`cleaning.py`/
# MAGIC `joins.py`/`aggregations.py` are reproduced directly as `@dlt.table`
# MAGIC functions below. Each docstring notes which package function it mirrors.
# MAGIC
# MAGIC Exam relevance: this notebook is the *Production Pipelines* section
# MAGIC (16% weight) of the Databricks Certified Data Engineer Associate exam.
# MAGIC
# MAGIC **This notebook is not run as a normal notebook/Job task** — it's added
# MAGIC as *source code* to a new **Lakeflow Declarative Pipeline** (previously
# MAGIC "Delta Live Tables") in the Databricks UI, which executes these
# MAGIC `@dlt.table` functions itself in dependency order.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

VOL = "/Volumes/workspace/default/dataforge_storage"
RAW = f"{VOL}/raw"

# Mirrors dataforge_ai.io.TARGET_TYPES — the schema-drift-safe cast map.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw, typed, unfiltered
# MAGIC
# MAGIC Mirrors `dataforge_ai.io.read_and_cast` + `read_trips`: read each
# MAGIC monthly file with its own native schema, lowercase columns (fixes the
# MAGIC `airport_fee`/`Airport_fee` casing clash from notebook 01), then
# MAGIC `.cast()` to `TARGET_TYPES` — the same in-engine-cast pattern that
# MAGIC avoids the physical-conversion errors hit in notebook 08.

# COMMAND ----------

@dlt.table(
    name="bronze_trips",
    comment="Raw NYC TLC trips, cast to a uniform schema, unfiltered.",
)
def bronze_trips():
    paths = [
        f"{RAW}/yellow_tripdata_2023-01.parquet",
        f"{RAW}/yellow_tripdata_2023-02.parquet",
        f"{RAW}/yellow_tripdata_2023-03.parquet",
    ]

    def read_and_cast(path):
        df = spark.read.parquet(path)
        df = df.toDF(*[c.lower() for c in df.columns])
        for col, target in TARGET_TYPES.items():
            df = df.withColumn(col, F.col(col.lower()).cast(target))
        return df.select(*TARGET_TYPES.keys())

    from functools import reduce
    df = reduce(lambda a, b: a.unionByName(b), [read_and_cast(p) for p in paths])
    return df.withColumn(
        "pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zones dimension table
# MAGIC
# MAGIC The taxi zone lookup CSV, needed for Silver's enrichment join.

# COMMAND ----------

@dlt.table(
    name="zones",
    comment="Taxi zone lookup dimension (LocationID -> Zone/Borough).",
)
def zones():
    return (
        spark.read.option("header", True)
        .csv(f"{RAW}/taxi_zone_lookup.csv")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: cleaned + zone-enriched, with tracked data-quality rules
# MAGIC
# MAGIC Each `@dlt.expect_or_drop` below is one rule from
# MAGIC `dataforge_ai.cleaning.clean_trips`, now individually named and
# MAGIC tracked by the pipeline (visible per-run in the pipeline UI) instead of
# MAGIC being an opaque `.filter()` chain. `expect_or_drop` drops violating
# MAGIC rows but keeps the pipeline running — matching `clean_trips`'s
# MAGIC behavior of silently filtering bad rows.
# MAGIC
# MAGIC Two rules from `clean_trips` that aren't simple boolean predicates —
# MAGIC the passenger-count-mode-fill and the exact-duplicate drop — are kept
# MAGIC as plain transformations after the expectations, since `@dlt.expect_*`
# MAGIC is for pass/fail row constraints, not value substitution or dedup.

# COMMAND ----------

@dlt.table(
    name="silver_trips_enriched",
    comment="Cleaned, zone-enriched trips: Jan-Mar 2023 only, quality rules "
    "enforced as tracked DLT expectations.",
)
@dlt.expect_or_drop("valid_amounts", "fare_amount >= 0 AND total_amount >= 0")
@dlt.expect_or_drop(
    "valid_trip_time", "tpep_dropoff_datetime >= tpep_pickup_datetime"
)
@dlt.expect_or_drop(
    "in_labelled_range",
    "tpep_pickup_datetime >= '2023-01-01' AND tpep_pickup_datetime < '2023-04-01'",
)
@dlt.expect_or_drop("realistic_distance", "trip_distance <= 100")
@dlt.expect_or_drop(
    "distance_or_fare_charged", "trip_distance > 0 OR fare_amount > 0"
)
def silver_trips_enriched():
    trips = dlt.read("bronze_trips")
    zones_df = dlt.read("zones")

    trips = trips.withColumn(
        "passenger_count",
        F.when(
            F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
            F.lit(1),
        ).otherwise(F.col("passenger_count")),
    )

    pu = zones_df.alias("pu")
    do = zones_df.alias("do")
    enriched = (
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

    return enriched.dropDuplicates([
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: hourly demand per borough
# MAGIC
# MAGIC Mirrors `dataforge_ai.aggregations.hourly_demand` exactly — same
# MAGIC `groupBy` + `dense_rank`/running-total window logic that produced 192
# MAGIC rows (24 hours x 8 boroughs) and Manhattan's 18:00 peak in notebook 06.

# COMMAND ----------

from pyspark.sql import Window


@dlt.table(
    name="gold_hourly_demand",
    comment="Trip count/avg fare per borough+hour, demand rank, running total.",
)
def gold_hourly_demand():
    trips = dlt.read("silver_trips_enriched").withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    )

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up the pipeline
# MAGIC
# MAGIC In the Databricks UI: **Jobs & Pipelines -> Create -> ETL Pipeline**
# MAGIC (Lakeflow Declarative Pipelines), add this notebook as source code,
# MAGIC set the target catalog/schema (e.g. `workspace.default`), serverless
# MAGIC compute, then **Start**. The pipeline reads the `@dlt.table`
# MAGIC decorators, infers `zones`/`bronze_trips` -> `silver_trips_enriched` ->
# MAGIC `gold_hourly_demand` as the dependency graph, and runs them in that
# MAGIC order automatically — no manual "Depends on" wiring like the Job.
# MAGIC
# MAGIC After a run, the pipeline's graph view shows each table plus, on
# MAGIC `silver_trips_enriched`, a data-quality panel with pass/fail/drop
# MAGIC counts *per expectation* — e.g. how many rows `valid_amounts` alone
# MAGIC dropped, distinct from `realistic_distance`. The Job (07a/b/c) has no
# MAGIC equivalent — its row-count message is a single aggregate number.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Same medallion output as 06/07a-c: Bronze unfiltered, Silver cleaned
# MAGIC   + enriched (Jan-Mar 2023 only), Gold = 192 rows of hourly demand.
# MAGIC - Dependency graph (`zones`+`bronze_trips` -> `silver_trips_enriched`
# MAGIC   -> `gold_hourly_demand`) is inferred from `dlt.read()` calls, not
# MAGIC   manually wired in a UI like the Job's "Depends on" setting.
# MAGIC - `clean_trips`'s boolean filtering rules are now five named,
# MAGIC   independently tracked `@dlt.expect_or_drop` constraints instead of
# MAGIC   one opaque `.filter()` chain — the pipeline UI shows a drop count
# MAGIC   per rule, per run.
# MAGIC - Trade-off: DLT's managed execution model doesn't support
# MAGIC   `dbutils.library.restartPython()`, so the shared `dataforge_ai`
# MAGIC   package logic had to be reproduced inline here rather than imported
# MAGIC   — a real constraint of the declarative-pipeline model worth knowing
# MAGIC   for the exam and in practice.
# MAGIC - Next in the exam-prep gap round: Unity Catalog governance (real
# MAGIC   catalog/schema structure, `GRANT`/`REVOKE`, external locations).
