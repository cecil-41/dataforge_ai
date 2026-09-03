# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase 1 — Databricks Medallion Pipeline (overview)
# MAGIC
# MAGIC This single-notebook version has been split into three separate
# MAGIC notebooks so they can run as independent, dependent tasks in one
# MAGIC Databricks Job (Workflows tab) — the realistic production pattern for
# MAGIC a medallion pipeline:
# MAGIC
# MAGIC - [`07a_bronze.py`](./07a_bronze.py) — install package, fetch raw data
# MAGIC   into a Unity Catalog Volume, build Bronze
# MAGIC - [`07b_silver.py`](./07b_silver.py) — clean + zone-enrich Bronze into
# MAGIC   Silver (Job dependency: runs after `07a_bronze.py`)
# MAGIC - [`07c_gold.py`](./07c_gold.py) — aggregate Silver into Gold (Job
# MAGIC   dependency: runs after `07b_silver.py`)
# MAGIC
# MAGIC Each notebook is a separate Job task with its own dependency edge
# MAGIC (Bronze -> Silver -> Gold), so any layer can be retried/monitored
# MAGIC independently instead of one failure rerunning the whole pipeline.
# MAGIC
# MAGIC Same environment-specific differences from the local Docker version
# MAGIC apply to all three:
# MAGIC
# MAGIC - No `SparkSession.builder` -- Databricks injects a ready-to-use `spark`
# MAGIC   session automatically.
# MAGIC - No `configure_spark_with_delta_pip(...)` -- Delta Lake is built into
# MAGIC   the Databricks runtime, no JAR resolution needed.
# MAGIC - Paths use a **Unity Catalog Volume**
# MAGIC   (`/Volumes/workspace/default/dataforge_storage/...`) instead of local
# MAGIC   relative paths. DBFS is disabled on this workspace, and serverless
# MAGIC   compute can't read local `file:///tmp/...` paths either -- Volumes
# MAGIC   are the supported place for raw files + Delta tables here.
# MAGIC - The `dataforge_ai` package is installed from this same Git folder via
# MAGIC   `%pip install -e`, so `read_trips`/`clean_trips`/`join_zones`/
# MAGIC   `hourly_demand` are the exact same tested functions as the local
# MAGIC   pipeline -- no logic duplicated or rewritten for Databricks.
# MAGIC
# MAGIC Validated end-to-end: Bronze/Silver/Gold row counts match the local
# MAGIC Docker pipeline (notebook 06) exactly (Silver = 9,301,798, Gold = 192,
# MAGIC Manhattan busiest hour = 18:00).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up the Job
# MAGIC
# MAGIC In the Workflows tab: **Create Job** -> add three Notebook tasks
# MAGIC (`bronze`, `silver`, `gold`) pointing at `07a_bronze.py`,
# MAGIC `07b_silver.py`, `07c_gold.py` respectively -> set `silver`'s
# MAGIC "Depends on" to `bronze`, and `gold`'s "Depends on" to `silver`. Each
# MAGIC task can use its own serverless compute, retry policy, and be
# MAGIC monitored/re-run independently in the Job run view.