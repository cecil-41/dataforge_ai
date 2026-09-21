# DataForge AI — Roadmap Progress

Mark an item complete only when it has been **implemented and demonstrated**
in the project, not merely discussed. See [learning_plan.md](learning_plan.md)
for the full phase descriptions.

## Phase 0 — Foundations
- [x] Local dev environment (Docker + JupyterLab + Spark, `docker compose up -d`)
- [x] Dataset acquired (NYC TLC Yellow Taxi, Jan–Mar 2023, ~9M rows)
- [x] PySpark fundamentals (SparkSession, driver/executors, lazy evaluation,
      partitions, transformations vs. actions):
      [notebooks/01_fundamentals.ipynb](../notebooks/01_fundamentals.ipynb)
- [x] Cleaning/ETL (nulls, dedup, invalid-row filtering, audited row removal):
      [notebooks/02_cleaning_and_etl.ipynb](../notebooks/02_cleaning_and_etl.ipynb)
- [ ] Advanced SQL (window functions, CTEs, query optimisation, execution plans)
- [x] PySpark joins (broadcast vs. shuffle, verified via `.explain()`,
      `left` vs `inner` via `left_anti`):
      [notebooks/03_joins.ipynb](../notebooks/03_joins.ipynb)
- [x] Aggregations & window functions in Spark (`groupBy`+`rank`/`dense_rank`,
      running totals via ordered window frames):
      [notebooks/04_aggregations_and_windows.ipynb](../notebooks/04_aggregations_and_windows.ipynb)
- [x] Spark performance tuning (skew vs. shuffle cost measured via
      `spark_partition_id()`, caching cost/benefit timed, AQE on/off
      compared; also resolved a `spark.driver.memory` local-mode gotcha via
      `SPARK_DRIVER_MEMORY` env var in `docker-compose.yml`):
      [notebooks/05_performance_tuning.ipynb](../notebooks/05_performance_tuning.ipynb)
- [x] Testing & clean project structure (pytest, `src/dataforge_ai` package):
      editable-installed package (`pip install -e .`) with
      `io.py`/`cleaning.py`/`joins.py`/`aggregations.py`, unit-tested against
      small synthetic DataFrames (6 tests, all passing in ~9s) via
      `local[1]` fixture in [tests/conftest.py](../tests/conftest.py);
      notebooks 03-05 now import shared logic from the package instead of
      duplicating it, notebook 02 keeps its own `clean_trips` (teaching
      origin) but imports `TARGET_TYPES`/`read_and_cast`:
      [src/dataforge_ai](../src/dataforge_ai), [tests](../tests)

## Phase 1 — Databricks + Delta Lake
- [x] Databricks workspace: Free Edition (serverless compute, no classic
      manually-configured clusters). Two gotchas discovered porting
      [notebooks/07a_bronze.py](../notebooks/07a_bronze.py) /
      [07b_silver.py](../notebooks/07b_silver.py) /
      [07c_gold.py](../notebooks/07c_gold.py) from the local pipeline:
      (1) `%pip install -e .` resolves relative to the notebook's own
      folder in a Git folder, not the repo root — fixed by installing from
      the repo root path explicitly; (2) **DBFS is disabled** on this
      workspace and serverless compute cannot read local `file:///tmp/...`
      paths either — Spark on serverless only sees `/Workspace/` and
      **Unity Catalog Volumes**, so raw data + Delta tables now live under
      a UC Volume (`/Volumes/workspace/default/dataforge_storage`) instead
      of `dbfs:/` paths. Pipeline run validated end-to-end on Databricks:
      Silver = 9,301,798 rows, Gold = 192 rows -- both match the local
      Docker pipeline (notebook 06) exactly
- [x] Delta Lake (ACID transaction log verified via commit JSON inspection +
      `DeltaTable.history()`; schema evolution via `mergeSchema`; time travel
      via `versionAsOf`; `MERGE` upserts via `whenMatchedUpdate` +
      `whenNotMatchedInsertAll`, both confirmed against real row-count deltas
      and `operationMetrics`):
      [notebooks/06_delta_lake_fundamentals.ipynb](../notebooks/06_delta_lake_fundamentals.ipynb)
- [x] Medallion architecture (bronze/silver/gold): Bronze built from
      `read_trips` (unfiltered, partitioned by `pickup_month`, preserves
      known bad TLC timestamps by design), Silver from `clean_trips` +
      `join_zones` (date-range + quality filters applied, 3 valid months
      only), Gold from `hourly_demand` (192 rows = 24 hours × 8 boroughs,
      Manhattan's busiest hour = 18:00 matches notebook 04's result exactly)
      — all three reuse `dataforge_ai` package functions unchanged, only the
      storage layer is new:
      [notebooks/06_delta_lake_fundamentals.ipynb](../notebooks/06_delta_lake_fundamentals.ipynb)
- [x] Databricks jobs, workflows, cluster config: `dataforge_medallion` Job
      with three dependent Notebook tasks (`bronze` -> `silver` -> `gold`,
      each `Depends on` the previous, `Run if: All succeeded`, serverless
      compute per task) — mirrors real medallion pipelines where each layer
      is independently retryable/observable/schedulable instead of one
      monolithic run. Two serverless-specific gotchas hit and fixed along
      the way: (1) `%pip install -e` magic-command variable interpolation
      is unreliable across interactive vs. Job-task execution — fixed by
      installing via `subprocess.check_call` on a dynamically-resolved
      absolute repo-root path instead; (2) `dbutils.library.restartPython()`
      wipes the `sys.path` entry the editable install added on serverless
      compute (documented Databricks issue) — fixed by re-appending
      `{repo_root}/src` to `sys.path` after the restart, recomputed fresh
      each time rather than hardcoded. Full Job run succeeded end-to-end in
      1m 48s, all three tasks green
- [x] Structured Streaming fundamentals (core mechanics validated locally in
      [notebooks/08_structured_streaming.ipynb](../notebooks/08_structured_streaming.ipynb)):
      `rate` source micro-batch model, output modes, triggers
      (`processingTime`, `availableNow`), checkpoint-based incremental
      ingestion (proven on the first file arrival), windowed aggregation +
      watermarking for Gold. **Known limitation discovered, not resolved
      locally:** plain Spark's file streaming source forces one fixed
      schema across every file in a directory — it can't handle the NYC
      TLC dataset's genuine per-month physical-type drift on `VendorID`
      (already documented in notebook 01) the way batch's `read_and_cast`
      does (reads each file with its own native schema, casts after).
      Disabling the vectorized Parquet reader did not fix it. This is
      exactly the gap Databricks Auto Loader's schema evolution + rescued
      data column exist to solve — deferred to the Databricks Auto Loader
      port rather than solved with plain OSS Structured Streaming
- [x] Unity Catalog — the Free Edition workspace already provisions a
      default `workspace` catalog with UC Volumes enabled; used directly
      above for raw data + Delta table storage in place of DBFS. Deeper
      governance features (external locations, fine-grained access control)
      still deferred to Phase 2's full Azure Databricks workspace

### Databricks Certified Data Engineer Associate — exam-prep gap round
Closing gaps identified against the official exam guide's weighted sections
before sitting the exam (each item is a real DataForge AI feature, not just
exam trivia):
- [x] Auto Loader (`cloudFiles`, schema evolution + `_rescued_data`) — built
      and validated on Databricks in
      [notebooks/09_databricks_autoloader.py](../notebooks/09_databricks_autoloader.py).
      Ran as the `autoloader_bronze` task, kept independent of the
      `dataforge_medallion` bronze/silver/gold DAG (no real data
      dependency between them). Confirmed the fix: the February file
      (physically `INT32` `VendorID` vs. January's `bigint`) no longer
      crashes the stream — all 2,913,955 February rows had `VendorID`
      routed into `_rescued_data` (as JSON, alongside `passenger_count`,
      `RatecodeID`, etc.) with the column itself `NULL`, matching the
      exact scenario that threw `SchemaColumnConvertNotSupportedException`
      in notebook 08's plain Structured Streaming pipeline. Directly
      resolves the *Incremental Data Processing* exam gap (22% weight)
- [ ] Lakeflow Declarative Pipelines (DLT): rebuild the medallion Job
      declaratively with `EXPECT` data-quality constraints — targets
      *Production Pipelines* (16%)
- [ ] Unity Catalog governance: real catalog/schema structure, `GRANT`/
      `REVOKE`, external locations/storage credentials (beyond the default
      catalog used so far) — targets *Data Governance* (9%)

## Phase 2 — Azure + modern data stack
- [ ] Azure Data Factory
- [ ] Azure Data Lake Storage Gen2
- [ ] Azure Databricks
- [ ] Azure Key Vault / Synapse
- [ ] dbt
- [ ] Terraform (for DataForge AI infra)
- [ ] CI/CD (GitHub Actions / Azure DevOps)

## Phase 3 — AI layer
- [ ] Embeddings & vector databases
- [ ] RAG pipeline
- [ ] LLM evaluation
- [ ] MLOps fundamentals

## Phase 4 — Production hardening + portfolio
- [ ] Monitoring, logging, error handling
- [ ] Architecture diagrams, docs, README
- [ ] Portfolio polish / write-up

## Tooling meta (not part of the data stack, but worth tracking)
- [x] Custom agent set up (`DataForge Mentor`) + repo-wide instructions
