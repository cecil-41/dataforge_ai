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
- [ ] Databricks workspace: Community Edition (free, no expiry; sufficient for
      everything except Unity Catalog — see note below)
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
- [ ] Databricks jobs, workflows, cluster config
- [ ] Structured Streaming fundamentals
- [ ] Unity Catalog — concept only in Phase 1 (not supported on Community
      Edition); hands-on deferred to Phase 2 Azure Databricks workspace

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
