# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase 1 — Silver: cleaned + zone-enriched
# MAGIC
# MAGIC Second task in the Bronze -> Silver -> Gold Databricks Job. Runs only
# MAGIC after `07a_bronze.py` succeeds (Job task dependency), so the Bronze
# MAGIC Delta table and the raw zone lookup CSV already exist in the Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the `dataforge_ai` package from this Git folder
# MAGIC
# MAGIC Resolving the repo root as an absolute path and installing via
# MAGIC `subprocess` -- instead of `%pip install -e {var}` magic-command
# MAGIC variable interpolation -- makes this reliable both when run
# MAGIC interactively and as a Databricks Job task.

# COMMAND ----------

import os
import subprocess
import sys

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + os.path.dirname(notebook_dir)
print("Repo root:", repo_root)

subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", repo_root])
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Silver

# COMMAND ----------

from pyspark.sql import functions as F
from dataforge_ai import clean_trips, join_zones

VOL = "/Volumes/workspace/default/dataforge_storage"
RAW = f"{VOL}/raw"
DELTA_ROOT = f"{VOL}/delta"
BRONZE_PATH = f"{DELTA_ROOT}/bronze/trips"
SILVER_PATH = f"{DELTA_ROOT}/silver/trips_enriched"

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
# MAGIC ## Recap
# MAGIC
# MAGIC - `clean_trips` + `join_zones` are the exact same tested functions used
# MAGIC   locally — no logic duplicated for Databricks.
# MAGIC - Validated: 9,301,798 rows, matching the local Docker pipeline
# MAGIC   (notebook 06) exactly.
# MAGIC - Next task in the Job: `07c_gold.py`.
