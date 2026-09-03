# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase 1 — Databricks Medallion Pipeline
# MAGIC
# MAGIC Databricks-native port of `notebooks/06_delta_lake_fundamentals.ipynb`.
# MAGIC Same Bronze -> Silver -> Gold logic, reusing the `dataforge_ai` package
# MAGIC unchanged -- only the environment-specific bits differ from the local
# MAGIC Docker version:
# MAGIC
# MAGIC - No `SparkSession.builder` -- Databricks injects a ready-to-use `spark`
# MAGIC   session automatically.
# MAGIC - No `configure_spark_with_delta_pip(...)` -- Delta Lake is built into
# MAGIC   the Databricks runtime, no JAR resolution needed.
# MAGIC - Paths use DBFS (`/dbfs/...` for plain Python, `dbfs:/...` for Spark
# MAGIC   readers/writers) instead of local relative paths.
# MAGIC - The `dataforge_ai` package is installed from this same Git folder via
# MAGIC   `%pip install -e`, so `read_trips`/`clean_trips`/`join_zones`/
# MAGIC   `hourly_demand` are the exact same tested functions as the local
# MAGIC   pipeline -- no logic duplicated or rewritten for Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the `dataforge_ai` package from this Git folder
# MAGIC
# MAGIC `%pip install -e` here works the same way the Docker image's editable
# MAGIC install does -- points at the package's `pyproject.toml` in this cloned
# MAGIC repo. `%pip` restarts the Python process on this cluster when it
# MAGIC finishes, so it must run before any other imports.

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %pip install -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch the dataset into DBFS
# MAGIC
# MAGIC Same source URLs as `scripts/download_data.py`, landed under
# MAGIC `/dbfs/tmp/dataforge_raw` since raw data isn't (and shouldn't be)
# MAGIC committed to the repo.

# COMMAND ----------

# DBTITLE 1,Cell 5
import os
import requests

# DBFS is disabled on this workspace — download directly to a UC Volume
# so Spark can read the files without a local-to-volume copy step.
VOL = "/Volumes/workspace/default/dataforge_storage"
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.dataforge_storage")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MONTHS = ["2023-01", "2023-02", "2023-03"]

RAW_DIR = f"{VOL}/raw"
os.makedirs(RAW_DIR, exist_ok=True)


def download(url: str, dest: str) -> None:
    if os.path.exists(dest):
        print(f"[skip] {os.path.basename(dest)} already exists")
        return
    print(f"[download] {url}")
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    print(f"  -> {dest}")


download(LOOKUP_URL, f"{RAW_DIR}/taxi_zone_lookup.csv")
for month in MONTHS:
    download(f"{BASE_URL}/yellow_tripdata_{month}.parquet", f"{RAW_DIR}/yellow_tripdata_{month}.parquet")

print("\nFiles:", os.listdir(RAW_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: paths + imports
# MAGIC
# MAGIC `spark` is already available -- no `SparkSession.builder` needed.
# MAGIC Delta is the default table format on Databricks, so no
# MAGIC `spark.sql.extensions` config is required either.

# COMMAND ----------

# DBTITLE 1,Cell 7
import sys
sys.path.insert(0, "/Workspace/Users/cecilbennett41@gmail.com/dataforge_ai/src")

from pyspark.sql import functions as F
from dataforge_ai import read_trips, clean_trips, join_zones, hourly_demand

VOL = "/Volumes/workspace/default/dataforge_storage"
RAW = f"{VOL}/raw"
DELTA_ROOT = f"{VOL}/delta"
BRONZE_PATH = f"{DELTA_ROOT}/bronze/trips"
SILVER_PATH = f"{DELTA_ROOT}/silver/trips_enriched"
GOLD_PATH = f"{DELTA_ROOT}/gold/hourly_demand"

paths = [
    f"{RAW}/yellow_tripdata_2023-01.parquet",
    f"{RAW}/yellow_tripdata_2023-02.parquet",
    f"{RAW}/yellow_tripdata_2023-03.parquet",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw, typed, unfiltered

# COMMAND ----------

# DBTITLE 1,Cell 9
trips_raw = read_trips(spark, paths).withColumn(
    "pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM")
)

(
    trips_raw.write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_month")
    .save(BRONZE_PATH)
)

bronze = spark.read.format("delta").load(BRONZE_PATH)
print("Bronze rows:", bronze.count())
bronze.groupBy("pickup_month").count().orderBy("pickup_month").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: cleaned + zone-enriched

# COMMAND ----------

zones = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{RAW}/taxi_zone_lookup.csv")
)

bronze = spark.read.format("delta").load(BRONZE_PATH)

trips_silver = (
    join_zones(clean_trips(bronze), zones)
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
)

(
    trips_silver.write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_month")
    .save(SILVER_PATH)
)

silver = spark.read.format("delta").load(SILVER_PATH)
print("Silver rows:", silver.count())
silver.select(
    "pickup_zone", "pickup_borough", "dropoff_zone", "pickup_hour", "fare_amount"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: aggregated, business-level

# COMMAND ----------

silver = spark.read.format("delta").load(SILVER_PATH)

gold_hourly_demand = hourly_demand(silver)

(
    gold_hourly_demand.write.format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

gold = spark.read.format("delta").load(GOLD_PATH)
print("Gold rows:", gold.count())
gold.filter(F.col("pickup_borough") == "Manhattan").orderBy("pickup_hour").show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: transaction log + history
# MAGIC
# MAGIC Unlike the local Docker version, `DESCRIBE HISTORY delta.\`<path>\`` works
# MAGIC fine here with no relative-path issues, since DBFS paths are already
# MAGIC absolute (`dbfs:/...`).

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Same `dataforge_ai` package, same Bronze/Silver/Gold logic as the
# MAGIC   local Docker pipeline -- only the session bootstrap, package install,
# MAGIC   and paths changed for the Databricks environment.
# MAGIC - Delta Lake needed zero extra configuration here (native to the
# MAGIC   runtime), versus `configure_spark_with_delta_pip` locally.
# MAGIC - Next: turn this notebook into a **Databricks Job** (Workflows tab),
# MAGIC   explore scheduling/triggers, and look at cluster/compute policy
# MAGIC   options available on this workspace.