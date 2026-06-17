# Module 5 — Data Platforms: Databricks for Data Engineering (Lecture Notes)

> DE Zoomcamp 2026, Module 5 (Databricks edition).
> These notes teach the concepts; the companion **GUIDE.md** is the hands-on lab,
> and **homework/homework.md** is the self-test. Read this top-to-bottom once, then
> use the cheat sheet at the end as a reference.

---

## 0. The big picture: why Databricks exists

Traditionally, data teams ran **two separate systems**:

- a **data lake** (cheap object storage — S3/GCS/ADLS — holding raw files like CSV/Parquet), great for data science but with no transactions, schema enforcement, or fast SQL; and
- a **data warehouse** (BigQuery/Snowflake/Redshift), great for fast governed SQL but expensive and bad at unstructured/ML data.

You ended up copying data back and forth, with two copies, two governance models, and constant drift.

**Databricks' big idea is the Lakehouse**: keep data in cheap object storage *as open files* (Parquet), but add a transactional layer on top (**Delta Lake**) so it behaves like a warehouse — ACID transactions, schema, fast SQL, governance — *without* a second system.

```
            ┌─────────────────────────────────────────────┐
            │              LAKEHOUSE (Databricks)          │
            │  Warehouse reliability + Lake economics      │
            ├─────────────────────────────────────────────┤
   SQL ▶    │  Unity Catalog  (governance, lineage, perms) │
   ML  ▶    │  Delta Lake     (ACID tables over Parquet)   │
   ETL ▶    │  Apache Spark   (distributed compute engine) │
            ├─────────────────────────────────────────────┤
            │  Cloud object storage (S3 / GCS / ADLS)      │
            └─────────────────────────────────────────────┘
```

Everything else in this lecture is a layer or tool sitting on this stack.

---

## 1. The platform anatomy

A few terms you'll see constantly:

- **Workspace** — your Databricks environment (a URL like
  `https://....gcp.databricks.com`). It holds notebooks, jobs, pipelines, and
  connects to compute and storage.
- **Compute** — the machines that actually run your code. Two flavors:
  - **Classic clusters** — VMs you (or the platform) size and start; you wait for
    them to spin up, and they live in *your* cloud account's network.
  - **Serverless** — Databricks-managed compute that starts in seconds; you don't
    manage VMs. Simpler, but with constraints (see the **egress gotcha** in §5).
- **Apache Spark** — the distributed engine underneath. You write PySpark/SQL and
  Spark parallelizes it across the cluster. The `spark` object is your entry point:
  `spark.read.table(...)`, `spark.sql(...)`.
- **Unity Catalog (UC)** — the governance layer (covered in §7). Data is addressed
  as **`catalog.schema.table`** (three-level namespace), e.g.
  `workspace.dev.bronze_yellow_taxi_raw`.

---

## 2. Delta Lake — the heart of the lakehouse

**Delta Lake** is a storage format = Parquet files + a transaction log (`_delta_log`).
That log is what upgrades dumb files into a real table.

What the transaction log buys you:

- **ACID transactions** — a write either fully succeeds or doesn't; readers never see
  half-written data.
- **Time travel** — every change is a versioned commit, so you can query the past:
  `SELECT * FROM t VERSION AS OF 3` or `... TIMESTAMP AS OF '2026-06-01'`.
- **Schema enforcement & evolution** — rejects bad-shaped writes; can evolve on purpose.
- **Upserts/deletes** — `MERGE`, `UPDATE`, `DELETE` on a lake (impossible with plain Parquet).

### 2.1 Write modes (know these cold — Homework Q2)

When you write a DataFrame, the **mode** decides what happens to existing data:

| Mode | Behavior | Use when |
|---|---|---|
| `append` | adds new rows, keeps everything | incremental loads |
| `overwrite` | **replaces the entire table** | full rebuild / bootstrap |
| `overwrite` + `replaceWhere` | replaces **only rows matching a predicate** | reprocess one window |

The third one is the key technique. To reprocess **February only** without touching
January or March:

```python
(df.write
   .mode("overwrite")
   .option("replaceWhere", "month = '2024-02'")   # atomic: delete Feb, insert new Feb
   .save(path))
```

This is *idempotent* — re-running it produces the same result, which is exactly what
you want in a pipeline. Plain `overwrite` would wipe Jan + Mar too; `append` would
duplicate February.

> In this project, `src/ingest_taxi.py` uses a blunt full `.mode("overwrite")` because
> it's a one-shot bootstrap of a single dataset — not a selective monthly reprocess.

### 2.2 The Medallion Architecture (Bronze → Silver → Gold)

A convention for organizing pipelines into quality layers:

```
RAW source ─▶  BRONZE  ─▶   SILVER    ─▶    GOLD
            (as-ingested)  (cleaned,      (business
             + metadata)    conformed)     aggregates)
```

- **Bronze** — raw data landed as-is, plus ingestion metadata. *Trust nothing yet.*
  → in this project: `workspace.dev.bronze_yellow_taxi_raw`.
- **Silver** — filtered, deduped, typed, joined. *Clean and queryable.*
- **Gold** — aggregated, business-ready tables feeding dashboards/ML.
  → in this project: `sample_zones_de_zoomcamp_2026` (fares summed by zip).

Each layer is a Delta table; you progressively refine data left-to-right.

---

## 3. Working with data: notebooks, Volumes, and parameters

### 3.1 Notebooks
Interactive documents (Python/SQL/Scala/R cells) attached to compute. The default DE
workflow: prototype in a notebook, then promote logic into source files/jobs.

### 3.2 Volumes — governed files inside Unity Catalog
Not everything is a table (e.g. raw downloaded files, images, model artifacts).
A **Volume** is a UC-governed folder you access by path:
`/Volumes/<catalog>/<schema>/<volume>/...`. It's the governed replacement for the old
DBFS. You'd stage a downloaded parquet into a Volume, then read it with Spark.

### 3.3 Notebook parameters with widgets (Homework Q3)
To make a notebook reusable — with a default that a job/caller can override at runtime —
use **widgets**, *not* env vars or Spark conf:

```python
dbutils.widgets.text("taxi_types", '["yellow","green"]')   # declare + default
taxi_types = dbutils.widgets.get("taxi_types")             # read (override-able)
```

When a Job runs the notebook, the caller can pass a different `taxi_types` and it flows
straight into `dbutils.widgets.get`. (`os.environ`, `spark.conf.get`, `sc.getConf()` are
*not* the parameter mechanism.)

---

## 4. Orchestration: Databricks Workflows (Jobs)

A **Job** (a.k.a. Workflow) is how you schedule and chain work. A job has one or more
**tasks**, each running a notebook, Python file, SQL query, or pipeline. Tasks form a
**DAG** via dependencies, so a step waits for its upstream steps.

In this project (`resources/jobs.yml`), `ingest_taxi_job` has a single
`spark_python_task` running `src/ingest_taxi.py` on a serverless environment.

### 4.1 Repair Run (Homework Q4)
When a multi-task job fails partway through, you do **not** start over. Use **Repair
Run** on the failed run: pick the task to restart from, and it re-runs that task plus
everything downstream while **reusing** the already-successful upstream results. Saves
compute and preserves run history.

```
[ingest] ✅ ─▶ [transform] ❌ ─▶ [load] ⛔(skipped)
                 └── Repair Run from here ──▶ re-runs transform + load only
```

---

## 5. Serverless networking — the gotcha you'll actually hit

Serverless compute is convenient but, **by default, has no public internet egress**.
Code that reaches out to the internet from the driver will fail with DNS errors:

```python
urllib.request.urlretrieve("https://.../yellow_tripdata_2024-01.parquet", path)
# URLError: <urlopen error [Errno -3] Temporary failure in name resolution>
```

Three ways to deal with it:
1. **Use workspace-local data** — e.g. the built-in `samples.nyctaxi.trips` dataset
   (what this project switched to). No network needed.
2. **Stage files via a machine that *does* have internet** — download locally, then
   `databricks fs cp file dbfs:/Volumes/<cat>/<schema>/<vol>/file`, and have the job
   read from the Volume.
3. **Configure serverless egress** (Network Connectivity Config / NCC) — the "proper
   cloud" answer, but heavier admin setup.

Lesson: **separate *acquiring* the data (needs internet) from *processing* it (runs on
serverless).** Land files first; transform second.

---

## 6. Infrastructure as code: Databricks Asset Bundles (DAB)

Clicking around the UI doesn't scale or reproduce. **Asset Bundles** let you define your
whole project — jobs, pipelines, configs, environments — as **YAML in git**, then deploy
it with the CLI. (This is the Databricks-native replacement for the course's Bruin/dlt
project config.)

### 6.1 The required file: `databricks.yml` (Homework Q1)
A folder is only recognized as a bundle if it has a **`databricks.yml`** (or
`bundle.yml`) at its root. It declares:

```yaml
bundle:
  name: databricks_taxi_pipeline      # the bundle's identity

include:
  - resources/*.yml                   # pull in jobs/pipelines defined elsewhere

variables:                            # parameterize across environments
  catalog: { description: The catalog to use }

targets:                              # the environments
  dev:
    mode: development                 # prefixes resources "[dev you]", pauses schedules
    default: true
    workspace: { host: https://....gcp.databricks.com }
    variables: { catalog: workspace, schema: dev }
  prod:
    mode: production                  # real names, schedules active
    workspace: { host: ..., root_path: /Workspace/Users/you/.bundle/... }
    variables: { catalog: workspace, schema: prod }
```

`requirements.txt`, `resources/jobs.yml`, `pipeline.yml` are all optional pieces —
without `databricks.yml` there's no bundle.

### 6.2 dev vs prod modes
- **development mode** — resources get a `[dev <user>]` prefix (so multiple people don't
  collide), and schedules/triggers are **paused**. Safe sandbox.
- **production mode** — real resource names, schedules **active**, stricter permissions.

### 6.3 The CLI workflow (memorize this loop)
```bash
databricks auth login --host <workspace-url>   # authenticate once
databricks bundle validate                     # check YAML resolves
databricks bundle deploy --target dev          # upload code + create resources
databricks bundle run <job_or_pipeline_key>    # execute
databricks bundle destroy --target dev         # tear down
```

---

## 7. Governance & lineage: Unity Catalog

**Unity Catalog (UC)** is the single governance layer over all data and AI assets.

### 7.1 The three-level namespace
Every table/view/volume lives at **`catalog.schema.object`**:

```
workspace                 ← catalog   (top-level container, often per-environment)
└── dev                   ← schema    (a.k.a. database; logical grouping)
    └── bronze_yellow_taxi_raw   ← table
```

UC centralizes **permissions** (GRANT/REVOKE at any level), **auditing**, **discovery**
(search across the org), and **lineage**.

### 7.2 Data lineage (Homework Q6)
UC **automatically** tracks where data came from and where it goes — at **table and
column level**. To view it: **Catalog Explorer → select the table → Lineage tab**. You
see upstream sources and downstream consumers, including column-level edges.

Don't confuse it with:
- **Job Task Graph** = orchestration order, not data flow.
- **Spark UI** = query execution internals.
- **Query History** = past SQL statements.

---

## 8. Declarative pipelines: Lakeflow (formerly Delta Live Tables / DLT)

### 8.1 Imperative vs declarative
With plain Spark jobs you write *how* to compute each step and wire dependencies
yourself. With **declarative pipelines** you just declare each table as a function that
returns a DataFrame; the framework infers the dependency DAG, manages incremental
processing, checkpoints, retries, and data-quality — automatically.

> **Naming (2025 rebrand):** **Delta Live Tables (DLT)** is now **Lakeflow Declarative
> Pipelines** (Python flavor: *Lakeflow Spark Declarative Pipelines*), aligning with
> Apache Spark Declarative Pipelines in Spark 4.1.
> Docs: <https://docs.databricks.com/aws/en/ldp/concepts/where-is-dlt>
> - Import: `import dlt` → **`from pyspark import pipelines as dp`**
> - Decorators: `@dlt.table` → **`@dp.table`**, `@dlt.view` → **`@dp.temporary_view`**,
>   new **`@dp.materialized_view`**; `@dp.table` now creates **streaming tables**.
> - **No migration required** — legacy `dlt` APIs still work. New code should use `dp`.

### 8.2 Defining tables (the modern `dp` style — used in this project)
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

@dp.table
def sample_trips_de_zoomcamp_2026():
    return spark.read.table("samples.nyctaxi.trips")          # a source table

@dp.table
def sample_zones_de_zoomcamp_2026():
    return (spark.read.table("sample_trips_de_zoomcamp_2026") # depends on the table above
            .groupBy(col("pickup_zip"))
            .agg(sum("fare_amount").alias("total_fare")))
```

The framework sees that `zones` reads `trips` and builds the DAG `trips ▶ zones`
for you — no manual wiring.

### 8.3 Expectations = data quality (Homework Q5)
Expectations are declarative constraints on rows. Three behaviors when a row violates:

| Decorator | On violation |
|---|---|
| `@dlt.expect(name, cond)` | **warn** — keep the row, record a metric |
| `@dlt.expect_or_drop(name, cond)` | **drop** the bad row, pipeline continues |
| `@dlt.expect_or_fail(name, cond)` | **fail the whole update immediately** |

To guarantee `pickup_datetime` is never NULL and **stop the run** if it is:
```python
@dlt.expect_or_fail("valid_dt", "pickup_datetime IS NOT NULL")
```
(`@dlt.constraint` isn't a real decorator.)

### 8.4 Update types & Full Refresh (Homework Q7)
- **Triggered** update — runs once and stops (batch cadence).
- **Continuous** update — keeps running, processing new data as it arrives (streaming).
- **Full Refresh** — **discards all state/checkpoints and reprocesses every source
  record from scratch.** Use it when stale checkpoints (e.g. from another environment)
  corrupt state, or after a breaking logic change.
- *Development mode* affects cluster reuse/retries — it does **not** clear checkpoints.

---

## 9. How it all fits together in this project

```
                         databricks.yml  (the bundle: dev/prod targets, vars)
                                │  includes
                ┌───────────────┴───────────────┐
        resources/jobs.yml                resources/...pipeline.yml
        (Workflow: ingest_taxi_job)       (Lakeflow Declarative Pipeline)
                │ runs                              │ runs
        src/ingest_taxi.py                 src/.../transformations/*.py
        reads samples.nyctaxi.trips        @dp.table sample_trips ─▶ sample_zones
        ▼ writes                                   ▼ materializes
   workspace.dev.bronze_yellow_taxi_raw    workspace.dev.sample_trips / sample_zones
        (BRONZE Delta table)                       (SILVER/GOLD Delta tables)
                          \                        /
                           ▼  all governed by  ▼
                        Unity Catalog (perms + auto lineage)
```

End-to-end: **author YAML (DAB) → deploy with CLI → a Job lands Bronze data → a Lakeflow
pipeline transforms it into Silver/Gold Delta tables → Unity Catalog governs and traces
everything.**

---

## 10. Cheat sheet

**Concepts**
- Lakehouse = warehouse reliability on lake storage, via **Delta Lake**.
- Delta = Parquet + transaction log ⇒ ACID, time travel, upserts, schema enforcement.
- Medallion: **Bronze** (raw) → **Silver** (clean) → **Gold** (aggregated).
- Unity Catalog namespace: **`catalog.schema.object`**; auto **lineage** in Catalog Explorer.
- Lakeflow Declarative Pipelines (ex-DLT): declare tables with `@dp.table`; framework builds the DAG.

**Commands**
```bash
databricks auth login --host <url>
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run <key>
databricks bundle destroy --target dev
```

**Code idioms**
```python
# selective reprocess (idempotent monthly overwrite)
df.write.mode("overwrite").option("replaceWhere", "month='2024-02'").save(path)

# notebook parameter
dbutils.widgets.text("taxi_types", '["yellow","green"]')
taxi_types = dbutils.widgets.get("taxi_types")

# declarative table + quality gate
from pyspark import pipelines as dp
@dp.table
@dlt.expect_or_fail("valid_dt", "pickup_datetime IS NOT NULL")
def trips():
    return spark.read.table("samples.nyctaxi.trips")
```

**Gotchas**
- `databricks.yml` is **required** for a bundle to exist.
- Serverless has **no internet egress** by default — land files first, process second.
- **Repair Run** restarts a failed task + downstream only (don't re-run the whole job).
- **Full Refresh** is the only update type that clears checkpoints/state.

**Homework answers:** Q1=B · Q2=C · Q3=C · Q4=C · Q5=C · Q6=B · Q7=C
*(see homework/homework.md for the questions and GUIDE.md Part B for the reasoning).*
