# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase 1 — Bronze: raw, typed, unfiltered
# MAGIC
# MAGIC First task in the Bronze -> Silver -> Gold Databricks Job. Fetches the
# MAGIC raw dataset into a Unity Catalog Volume (DBFS is disabled on this
# MAGIC workspace, and serverless compute can't read local `file:///tmp/...`
# MAGIC paths either), then builds the Bronze Delta table using the same
# MAGIC `dataforge_ai.read_trips` used by the local Docker pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the `dataforge_ai` package from this Git folder
# MAGIC
# MAGIC `%pip` restarts the Python process on this cluster when it finishes,
# MAGIC so it must run before any other imports.

# COMMAND ----------

# MAGIC %pip install -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch the dataset into a Unity Catalog Volume

# COMMAND ----------

import os
import requests

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
# MAGIC ## Build Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from dataforge_ai import read_trips

VOL = "/Volumes/workspace/default/dataforge_storage"
RAW = f"{VOL}/raw"
DELTA_ROOT = f"{VOL}/delta"
BRONZE_PATH = f"{DELTA_ROOT}/bronze/trips"

paths = [
    f"{RAW}/yellow_tripdata_2023-01.parquet",
    f"{RAW}/yellow_tripdata_2023-02.parquet",
    f"{RAW}/yellow_tripdata_2023-03.parquet",
]

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
# MAGIC ## Verify: transaction log + history
# MAGIC
# MAGIC `DESCRIBE HISTORY delta.\`<path>\`` works fine here since Volume paths
# MAGIC (`/Volumes/...`) are already absolute.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Bronze is unfiltered by design — known-bad TLC timestamps are
# MAGIC   preserved, not dropped, so Silver's filtering step is auditable.
# MAGIC - Next task in the Job: `07b_silver.py`, which reads this Delta table
# MAGIC   from the same Volume path.
