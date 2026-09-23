# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Auto Loader: incremental ingestion that survives schema drift
# MAGIC
# MAGIC [notebooks/08_structured_streaming.ipynb](../notebooks/08_structured_streaming.ipynb)
# MAGIC hit a real limitation of plain OSS Structured Streaming: its file source
# MAGIC forces **one fixed schema** across every file in a directory. When
# MAGIC February's file arrived with `VendorID` physically stored as `INT32`
# MAGIC instead of January's `INT64`/`bigint` (a genuine, documented drift in
# MAGIC this dataset — see notebook 01), the stream crashed with
# MAGIC `SchemaColumnConvertNotSupportedException` — no config fixed it, because
# MAGIC the problem is architectural, not a tuning knob.
# MAGIC
# MAGIC [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
# MAGIC (`format("cloudFiles")`) is Databricks' answer to exactly this: it
# MAGIC tracks an evolving schema in a `schemaLocation`, and with
# MAGIC `cloudFiles.schemaEvolutionMode = "rescue"`, any row whose data doesn't
# MAGIC match the tracked schema — new columns *or* type mismatches — gets
# MAGIC routed into a `_rescued_data` column (as JSON) instead of crashing the
# MAGIC stream. This is a Databricks-only feature (`cloudFiles` isn't available
# MAGIC in OSS Spark), so — like 07a/b/c — this notebook runs on Databricks, not
# MAGIC the local Docker Spark.
# MAGIC
# MAGIC Exam relevance: Auto Loader is core to the **Incremental Data
# MAGIC Processing** section (22% weight) of the Databricks Certified Data
# MAGIC Engineer Associate exam.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the `dataforge_ai` package from this Git folder
# MAGIC
# MAGIC Same pattern as 07a/b/c: resolve the repo root as an absolute path and
# MAGIC install via `subprocess` (not the `pip install -e {var}` magic command,
# MAGIC prefixed with `%`, which doesn't reliably interpolate variables), then
# MAGIC restart Python.

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
# MAGIC ## Re-add `src/` to `sys.path` after the restart
# MAGIC
# MAGIC `restartPython()` wipes all Python state, including the editable
# MAGIC install's `sys.path` entry (a known serverless-compute quirk — see
# MAGIC 07a's notes). Recomputed fresh since the restart also wiped `repo_root`.

# COMMAND ----------

import os
import sys

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + os.path.dirname(notebook_dir)
sys.path.append(f"{repo_root}/src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up a fresh landing directory + Auto Loader paths
# MAGIC
# MAGIC A separate `autoloader_landing` dir (distinct from 07a's `raw/`) lets us
# MAGIC control exactly when each file "arrives", same as notebook 08. Starting
# MAGIC with only the January file — the one with `VendorID` as `bigint`.

# COMMAND ----------

import shutil

VOL = "/Volumes/workspace/default/dataforge_storage"
RAW = f"{VOL}/raw"
LANDING_DIR = f"{VOL}/autoloader_landing"
SCHEMA_LOCATION = f"{VOL}/autoloader_schema"
CHECKPOINT = f"{VOL}/autoloader_checkpoint"
BRONZE_AUTOLOADER_PATH = f"{VOL}/delta/bronze/trips_autoloader"

# Clean slate: rerunning this notebook should behave like the first run.
for path in [LANDING_DIR, SCHEMA_LOCATION, CHECKPOINT, BRONZE_AUTOLOADER_PATH]:
    dbutils.fs.rm(path, recurse=True)

dbutils.fs.mkdirs(LANDING_DIR)

dbutils.fs.cp(
    f"{RAW}/yellow_tripdata_2023-01.parquet",
    f"{LANDING_DIR}/yellow_tripdata_2023-01.parquet",
)
print("Landing dir:", [f.name for f in dbutils.fs.ls(LANDING_DIR)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the Auto Loader stream (first file only)
# MAGIC
# MAGIC `cloudFiles.schemaEvolutionMode = "rescue"` is the key setting: instead
# MAGIC of failing hard on a schema mismatch, Auto Loader routes the offending
# MAGIC data into `_rescued_data` and keeps the stream alive.
# MAGIC `trigger(availableNow=True)` processes everything currently in the
# MAGIC landing dir then stops — the same batch-like trigger used in
# MAGIC notebook 08 and the 07a/b/c Job tasks.

# COMMAND ----------

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(LANDING_DIR)
)

query = (
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .start(BRONZE_AUTOLOADER_PATH)
)
query.awaitTermination()

bronze_autoloader = spark.read.format("delta").load(BRONZE_AUTOLOADER_PATH)
print("Rows after January:", bronze_autoloader.count())
bronze_autoloader.select("VendorID").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second file arrives — February, with `VendorID` as `INT32`
# MAGIC
# MAGIC This is the exact scenario that crashed notebook 08's plain Structured
# MAGIC Streaming pipeline. Rerunning the same Auto Loader stream (same
# MAGIC checkpoint) should pick up only the new file and — instead of
# MAGIC throwing `SchemaColumnConvertNotSupportedException` — either coerce the
# MAGIC value or park the mismatch in `_rescued_data`, without killing the
# MAGIC stream.

# COMMAND ----------

dbutils.fs.cp(
    f"{RAW}/yellow_tripdata_2023-02.parquet",
    f"{LANDING_DIR}/yellow_tripdata_2023-02.parquet",
)
print("Landing dir:", [f.name for f in dbutils.fs.ls(LANDING_DIR)])

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(LANDING_DIR)
)

query = (
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .start(BRONZE_AUTOLOADER_PATH)
)
query.awaitTermination()

bronze_autoloader = spark.read.format("delta").load(BRONZE_AUTOLOADER_PATH)
print("Rows after February:", bronze_autoloader.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the drift was rescued, not silently dropped or crashed
# MAGIC
# MAGIC Rows where `_rescued_data` is non-null are the ones Auto Loader
# MAGIC couldn't cleanly fit into the tracked schema — that's the mechanism
# MAGIC that replaces a hard stream failure with an inspectable, queryable
# MAGIC record of the mismatch.

# COMMAND ----------

if "_rescued_data" in bronze_autoloader.columns:
    rescued = bronze_autoloader.filter("_rescued_data IS NOT NULL")
    print("Rescued rows:", rescued.count())
    rescued.select("VendorID", "_rescued_data").show(5, truncate=False)
else:
    print("No _rescued_data column — schema absorbed the drift without rescue.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Plain Structured Streaming (notebook 08) forces one fixed schema per
# MAGIC   stream and hard-crashes on physical-type drift — no config resolves
# MAGIC   this, because it's an architectural gap, not a tuning issue.
# MAGIC - Auto Loader tracks schema state externally (`schemaLocation`) and,
# MAGIC   with `schemaEvolutionMode = "rescue"`, absorbs mismatched data into
# MAGIC   `_rescued_data` instead of failing the stream — turning an unhandled
# MAGIC   exception into an inspectable, queryable data-quality signal.
# MAGIC - This is the production-correct way to ingest a dataset with known
# MAGIC   cross-file drift, and directly closes the Incremental Data
# MAGIC   Processing gap for the Databricks Certified Data Engineer Associate
# MAGIC   exam.
# MAGIC - Next: Lakeflow Declarative Pipelines (DLT) — rebuild the medallion
# MAGIC   Job declaratively with `EXPECT` data-quality constraints.
