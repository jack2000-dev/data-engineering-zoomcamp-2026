# Module 5 — Data Platforms: Databricks (Lecture Notes)

> DE Zoomcamp 2026, Module 5 (Databricks edition). This is the condensed module
> overview. The **full, detailed lecture** lives in
> [`homework/LECTURE.md`](homework/LECTURE.md); the hands-on bundle is under
> [`databricks_taxi_pipeline/`](databricks_taxi_pipeline/), and the self-test is
> [`homework/homework.md`](homework/homework.md).

---

## The one idea: the Lakehouse

Classic stacks split into a **data lake** (cheap object storage, no transactions/SQL)
and a **data warehouse** (fast governed SQL, expensive, bad at ML/unstructured) — forcing
two copies and two governance models. **Databricks' Lakehouse** keeps data as open Parquet
files in object storage but adds a transactional layer so it behaves like a warehouse —
no second system.

```
┌─────────────────────────────────────────────┐
│  Unity Catalog  (governance, lineage, perms) │
│  Delta Lake     (ACID tables over Parquet)   │
│  Apache Spark   (distributed compute engine) │
│  Cloud object storage (S3 / GCS / ADLS)      │
└─────────────────────────────────────────────┘
```

---

## The layers, in one line each

- **Delta Lake** — Parquet + a transaction log (`_delta_log`) ⇒ ACID, time travel
  (`VERSION AS OF`), schema enforcement, and `MERGE`/`UPDATE`/`DELETE` on a lake.
- **Medallion** — **Bronze** (raw + metadata) → **Silver** (cleaned/typed/joined) →
  **Gold** (business aggregates). Each layer is a Delta table.
- **Unity Catalog (UC)** — single governance layer; everything addressed as
  **`catalog.schema.object`** (e.g. `workspace.dev.bronze_yellow_taxi_raw`). Gives
  permissions, audit, discovery, and **automatic table+column lineage**.
- **Compute** — *classic clusters* (VMs you size/start) vs *serverless* (managed,
  starts in seconds, but **no public internet egress by default**).
- **Workflows (Jobs)** — schedule and chain tasks into a DAG; **Repair Run** restarts a
  failed task + its downstream only (don't re-run the whole job).
- **Lakeflow Declarative Pipelines** (ex-Delta Live Tables) — declare each table as a
  function returning a DataFrame (`@dp.table`); the framework infers the DAG, handles
  incremental processing, retries, and **expectations** (data-quality gates).
- **Asset Bundles (DAB)** — your whole project (jobs, pipelines, configs) as YAML in git,
  deployed via the CLI. A folder is a bundle only if it has **`databricks.yml`**.

---

## Key code idioms

```python
# Selective, idempotent reprocess — replace only February, leave Jan/Mar untouched
df.write.mode("overwrite").option("replaceWhere", "month='2024-02'").save(path)

# Notebook parameter (the override mechanism — NOT env vars / spark.conf)
dbutils.widgets.text("taxi_types", '["yellow","green"]')
taxi_types = dbutils.widgets.get("taxi_types")

# Declarative pipeline table + quality gate
from pyspark import pipelines as dp
@dp.table
@dlt.expect_or_fail("valid_dt", "pickup_datetime IS NOT NULL")
def trips():
    return spark.read.table("samples.nyctaxi.trips")
```

## The CLI loop

```bash
databricks auth login --host <workspace-url>
databricks bundle validate
databricks bundle deploy --target dev      # dev = "[dev you]" prefix, schedules paused
databricks bundle run <job_or_pipeline_key>
databricks bundle destroy --target dev
```

---

## Gotchas worth memorizing

- **`databricks.yml` is required** for a bundle to exist (`requirements.txt`,
  `jobs.yml`, `pipeline.yml` are optional).
- **Serverless has no internet egress** by default — *acquire* data on a networked
  machine, *process* it on serverless. Land files first (e.g. into a UC **Volume**),
  then read them.
- **Repair Run** ≠ re-run job; **Full Refresh** is the only update type that clears
  pipeline checkpoints/state.
- Write modes: `append` (incremental), `overwrite` (full rebuild),
  `overwrite + replaceWhere` (reprocess one window, idempotent).
- Lineage lives in **Catalog Explorer → Lineage tab** — don't confuse with the Job Task
  Graph (orchestration), Spark UI (execution), or Query History.

---

## This project at a glance

```
databricks.yml  (bundle: dev/prod targets, vars)
   ├── resources/jobs.yml  →  src/ingest_taxi.py
   │       reads samples.nyctaxi.trips → writes workspace.dev.bronze_yellow_taxi_raw (BRONZE)
   └── resources/*.pipeline.yml  →  transformations/*.py
           @dp.table sample_trips → sample_zones  (SILVER/GOLD, DAG auto-built)
                          ▼
                 all governed by Unity Catalog (perms + auto lineage)
```

**Homework answers:** Q1=B · Q2=C · Q3=C · Q4=C · Q5=C · Q6=B · Q7=C
→ full reasoning in [`homework/LECTURE.md`](homework/LECTURE.md).
