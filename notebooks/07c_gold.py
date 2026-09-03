# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase 1 — Gold: aggregated, business-level
# MAGIC
# MAGIC Third task in the Bronze -> Silver -> Gold Databricks Job. Runs only
# MAGIC after `07b_silver.py` succeeds (Job task dependency).

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
# MAGIC ## Build Gold
# MAGIC
# MAGIC `dbutils.library.restartPython()` above wipes all Python state,
# MAGIC including the `sys.path` entry the editable install added -- a known
# MAGIC Databricks serverless-compute behavior (the install's `.pth` file
# MAGIC isn't re-read after the restart). Re-adding `src/` to `sys.path` here,
# MAGIC recomputed the same way as before the restart, fixes it without
# MAGIC hardcoding a path.

# COMMAND ----------

import os
import sys

notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + os.path.dirname(notebook_dir)
sys.path.append(f"{repo_root}/src")

from pyspark.sql import functions as F
from dataforge_ai import hourly_demand

VOL = "/Volumes/workspace/default/dataforge_storage"
DELTA_ROOT = f"{VOL}/delta"
SILVER_PATH = f"{DELTA_ROOT}/silver/trips_enriched"
GOLD_PATH = f"{DELTA_ROOT}/gold/hourly_demand"

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
# MAGIC ## Recap
# MAGIC
# MAGIC - `hourly_demand` is the same tested function used locally and in
# MAGIC   notebook 06.
# MAGIC - Validated: 192 rows (24 hours x 8 boroughs), Manhattan's busiest hour
# MAGIC   = 18:00, matching the local Docker pipeline exactly.
# MAGIC - This completes the Bronze -> Silver -> Gold Databricks Job. Next:
# MAGIC   Structured Streaming fundamentals, and exploring cluster/compute
# MAGIC   policy options on this workspace.
