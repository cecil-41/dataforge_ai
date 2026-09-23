# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog governance: grants, dynamic views, external storage
# MAGIC
# MAGIC The last gap in the exam-prep round. Every previous notebook (07a/b/c,
# MAGIC 09, 10) used Unity Catalog's **auto-provisioned defaults**
# MAGIC (`workspace.default`, the pre-created `dataforge_storage` Volume) —
# MAGIC useful for building things fast, but it never touched the actual
# MAGIC governance surface: who can see/read/write what, and how UC secures
# MAGIC access to data that lives outside its own managed storage.
# MAGIC
# MAGIC [Unity Catalog docs](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
# MAGIC
# MAGIC Covers, in order:
# MAGIC 1. The catalog.schema.table three-level namespace, and why we're
# MAGIC    isolating this work in its own schema
# MAGIC 2. `GRANT`/`REVOKE`/`SHOW GRANTS` — the actual privilege model
# MAGIC 3. Dynamic views — UC's mechanism for row/column-level security
# MAGIC 4. External Locations + Storage Credentials — conceptual only here
# MAGIC    (Free Edition's storage is fully managed; this needs a real
# MAGIC    ADLS/S3 account, so it's deferred to Phase 2's Azure Databricks
# MAGIC    workspace where we'll actually have one)
# MAGIC
# MAGIC Exam relevance: *Data Governance* (9% weight) — the last uncovered
# MAGIC section of the Databricks Certified Data Engineer Associate exam.
# MAGIC
# MAGIC No `dataforge_ai` package dependency here — this is pure SQL DDL/DCL,
# MAGIC so none of the `subprocess`/`restartPython()` install machinery from
# MAGIC 07a/b/c/09 is needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Isolate this work in its own schema
# MAGIC
# MAGIC Free Edition provides exactly one catalog (`workspace`) — creating an
# MAGIC additional catalog needs a metastore admin and a storage root
# MAGIC configuration that isn't exposed here. A dedicated **schema** is the
# MAGIC right-sized isolation boundary instead: it keeps grants/views scoped
# MAGIC to this notebook's objects, without touching `default`'s existing
# MAGIC medallion tables.

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.governance_demo
    COMMENT 'Isolated schema for Unity Catalog governance practice: grants, dynamic views'
""")

# A small, fast table to practice grants against -- the zone lookup, not
# the 9M-row trips tables.
VOL = "/Volumes/workspace/default/dataforge_storage"
zones = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{VOL}/raw/taxi_zone_lookup.csv"
)
zones.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.governance_demo.zones_demo"
)

display(spark.sql("SELECT * FROM workspace.governance_demo.zones_demo LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grants: the actual privilege model
# MAGIC
# MAGIC UC's privilege model is hierarchical: a principal needs `USE CATALOG`
# MAGIC on the catalog *and* `USE SCHEMA` on the schema just to see objects
# MAGIC inside it, then object-level privileges (`SELECT`, `MODIFY`, `CREATE`)
# MAGIC on top. Granting `SELECT` on a table alone does nothing if the
# MAGIC principal can't `USE SCHEMA` its way to it.
# MAGIC
# MAGIC Free Edition is single-user, so there's no second teammate account to
# MAGIC grant to here — `account users` is a built-in group present in every
# MAGIC Databricks account (all users in the account are implicitly members),
# MAGIC which makes the mechanics demonstrable without needing one.

# COMMAND ----------

spark.sql("GRANT USE CATALOG ON CATALOG workspace TO `account users`")
spark.sql("GRANT USE SCHEMA ON SCHEMA workspace.governance_demo TO `account users`")
spark.sql("GRANT SELECT ON TABLE workspace.governance_demo.zones_demo TO `account users`")

display(spark.sql("SHOW GRANTS ON TABLE workspace.governance_demo.zones_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revoke: removing a privilege actually takes it away
# MAGIC
# MAGIC `SHOW GRANTS` before and after `REVOKE` should show `SELECT` gone from
# MAGIC the list — this is the enforcement point that makes UC's governance
# MAGIC real rather than advisory: a query attempted after this revoke, by a
# MAGIC principal with no other grant path, is denied at the catalog layer,
# MAGIC not just hidden in a UI.

# COMMAND ----------

spark.sql("REVOKE SELECT ON TABLE workspace.governance_demo.zones_demo FROM `account users`")

display(spark.sql("SHOW GRANTS ON TABLE workspace.governance_demo.zones_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row/column-level security: dynamic views
# MAGIC
# MAGIC Unity Catalog has no separate "row-level security" object — the
# MAGIC standard pattern is a **dynamic view**: a normal view whose `SELECT`
# MAGIC calls `current_user()`/`is_account_group_member()` in a `CASE WHEN` to
# MAGIC mask columns or filter rows based on *who's asking*, evaluated at
# MAGIC query time against the caller's real identity — not the view
# MAGIC creator's.
# MAGIC
# MAGIC Below: `Zone` is masked to `***` unless the caller is in `admins`.
# MAGIC Since the notebook's owner is the workspace admin, querying this view
# MAGIC interactively will show real values — that's expected; a teammate
# MAGIC without the `admins` grant would see `***` instead.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW workspace.governance_demo.zones_masked AS
    SELECT
        LocationID,
        CASE WHEN is_account_group_member('admins') THEN Zone ELSE '***' END AS Zone,
        Borough
    FROM workspace.governance_demo.zones_demo
""")

display(spark.sql("SELECT * FROM workspace.governance_demo.zones_masked LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Locations + Storage Credentials (conceptual — deferred)
# MAGIC
# MAGIC Every table so far — here and in 07a/b/c/09/10 — is either a
# MAGIC **managed table** (UC owns the physical storage path, e.g.
# MAGIC `zones_demo` above) or lives in a UC **Volume** backed by Databricks'
# MAGIC own managed storage. Neither touches UC's mechanism for governing
# MAGIC data in storage *you* own.
# MAGIC
# MAGIC That mechanism is two objects:
# MAGIC - **Storage Credential**: a managed identity/service principal UC uses
# MAGIC   to authenticate against your cloud storage account
# MAGIC - **External Location**: a governed pointer binding a URL (e.g. an
# MAGIC   ADLS Gen2 container path) to a Storage Credential, so UC can grant
# MAGIC   `READ FILES`/`WRITE FILES` on that location like any other securable
# MAGIC
# MAGIC Illustrative syntax only — **not run here**, since Free Edition's
# MAGIC storage is fully managed and doesn't expose a way to register your
# MAGIC own storage account:
# MAGIC ```sql
# MAGIC CREATE STORAGE CREDENTIAL dataforge_storage_cred
# MAGIC   WITH (AZURE_MANAGED_IDENTITY = '<managed-identity-resource-id>');
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION dataforge_external_data
# MAGIC   URL 'abfss://data@dataforgestorage.dfs.core.windows.net/'
# MAGIC   WITH (STORAGE CREDENTIAL dataforge_storage_cred);
# MAGIC
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION dataforge_external_data TO `account users`;
# MAGIC ```
# MAGIC Deferred to Phase 2's Azure Databricks workspace, where we'll have a
# MAGIC real ADLS Gen2 storage account + managed identity to register against.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Validated on Free Edition: schema-level isolation, the
# MAGIC   `USE CATALOG`/`USE SCHEMA`/object-privilege hierarchy, `GRANT` ->
# MAGIC   `SHOW GRANTS` -> `REVOKE` actually changing enforcement (not just a
# MAGIC   UI label), and a dynamic view masking a column by caller identity.
# MAGIC - Deferred, not fabricated: External Locations + Storage Credentials
# MAGIC   need a real cloud storage account Free Edition doesn't expose —
# MAGIC   documented as illustrative syntax, to be actually run in Phase 2.
# MAGIC - This closes the exam-prep gap round: Auto Loader (09), Lakeflow
# MAGIC   Declarative Pipelines (10), Unity Catalog governance (11) — all
# MAGIC   three Data Engineer Associate exam gaps identified are now built
# MAGIC   and validated (external locations noted as a Phase 2 follow-up).
# MAGIC - Next: sit the Databricks Certified Data Engineer Associate exam, or
# MAGIC   kick off Phase 2 (Azure Data Factory, ADLS Gen2, Terraform, CI/CD).
