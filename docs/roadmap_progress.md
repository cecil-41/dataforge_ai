# DataForge AI — Roadmap Progress

Mark an item complete only when it has been **implemented and demonstrated**
in the project, not merely discussed. See [learning_plan.md](learning_plan.md)
for the full phase descriptions.

## Phase 0 — Foundations
- [x] Local dev environment (Docker + JupyterLab + Spark, `docker compose up -d`)
- [x] Dataset acquired (NYC TLC Yellow Taxi, Jan–Mar 2023, ~9M rows)
- [x] PySpark fundamentals (SparkSession, driver/executors, lazy evaluation,
      partitions, transformations vs. actions):
      [notebooks/01_setup_and_fundamentals.ipynb](../notebooks/01_setup_and_fundamentals.ipynb)
- [x] Cleaning/ETL (nulls, dedup, invalid-row filtering, audited row removal):
      [notebooks/02_cleaning_and_etl.ipynb](../notebooks/02_cleaning_and_etl.ipynb)
- [ ] Advanced SQL (window functions, CTEs, query optimisation, execution plans)
- [ ] PySpark joins (broadcast vs. shuffle, skew) — up next
- [ ] Aggregations & window functions in Spark
- [ ] Spark performance tuning (shuffle partitions, caching, AQE, explain plans)
- [ ] Testing & clean project structure (pytest, `src/dataforge_ai` package)

## Phase 1 — Databricks + Delta Lake
- [ ] Databricks workspace: Community Edition (free, no expiry; sufficient for
      everything except Unity Catalog — see note below)
- [ ] Delta Lake (ACID, time travel, schema evolution)
- [ ] Medallion architecture (bronze/silver/gold)
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
