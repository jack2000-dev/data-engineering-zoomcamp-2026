# Modified Homework from Module 5

Replaced Bruin with Databricks

> **Note — "DLT" terminology (2025 rebrand).** Questions Q5–Q7 refer to **DLT
> (Delta Live Tables)**, which Databricks has since renamed to **Lakeflow Declarative
> Pipelines** (Python flavor: *Lakeflow Spark Declarative Pipelines*).
> See <https://docs.databricks.com/aws/en/ldp/concepts/where-is-dlt>.
> The concepts (expectations, lineage, full refresh) are unchanged. API naming moved
> from `import dlt` / `@dlt.table` to `from pyspark import pipelines as dp` / `@dp.table`,
> but the legacy `dlt` decorators in these questions **still work**, so the answers below
> remain valid.

## Q1: Bundle structure
In a Databricks Asset Bundle (DAB) project, which file is always required for the bundle to be recognized and defines environments (dev/prod) alongside workspace and resource references?

- [ ] A) requirements.txt
- [ ] B) databricks.yml
- [ ] C) resources/jobs.yml
- [ ] D) pipeline.yml

## Q2: Delta Lake write strategy
You process NYC taxi data organized by month. You need to reprocess February 2024 by deleting all existing February records and inserting fresh data — without affecting January or March data. Which Delta Lake write approach achieves this?

- [ ] A) df.write.mode("append").save(path) — always add new rows
- [ ] B) df.write.mode("overwrite").save(path) — truncate and rebuild entirely
- [ ] C) df.write.mode("overwrite").option("replaceWhere", "month = '2024-02'").save(path) — overwrite only the target window
- [ ] D) df.createOrReplaceTempView("trips") — create a virtual view only

## Q3: Notebook parameters
Your Databricks notebook processes taxi data and accepts a taxi_types parameter that should default to ["yellow", "green"] but allow job callers to override it at runtime. Which is the correct way to declare this parameter in the notebook?

- [ ] A) taxi_types = spark.conf.get("taxi_types", '["yellow","green"]')
- [ ] B) taxi_types = os.environ.get("TAXI_TYPES", "yellow,green")
- [ ] C) dbutils.widgets.text("taxi_types", '["yellow","green"]')
- [ ] D) taxi_types = sc.getConf().get("spark.taxi_types", "yellow")

## Q4: Re-running downstream tasks
You modified the ingest_trips task in a Databricks Workflow and a downstream transform_trips task failed because of the change. You want to re-run ingest_trips and all tasks that depend on it — without re-running earlier tasks that succeeded. What is the correct approach?

- A) Trigger a full new job run from the beginning
- [ ] B) Delete the failed run and submit an entirely new job
- [ ] C) Use Repair Run and mark ingest_trips as the first task to re-run
- [ ] D) Manually trigger each downstream task one at a time from the Workflows UI

## Q5: DLT data quality expectations
You want to ensure the pickup_datetime column in your Delta Live Tables pipeline never contains NULL values. If a NULL is detected, the pipeline run should fail immediately rather than silently dropping rows. Which DLT decorator should you use?

- [ ] A) @dlt.expect("valid_dt", "pickup_datetime IS NOT NULL")
- [ ] B) @dlt.expect_or_drop("valid_dt", "pickup_datetime IS NOT NULL")
- [ ] C) @dlt.expect_or_fail("valid_dt", "pickup_datetime IS NOT NULL")
- [ ] D) @dlt.constraint("pickup_datetime IS NOT NULL")

## Q6: Data lineage
After running your Delta Live Tables pipeline that produces a trips_agg table, you want to see which upstream source tables and columns contributed to it. Where do you find column-level lineage in the Databricks UI?

- [ ] A) Databricks UI → Workflows → Job Run → Task Graph
- [ ] B) Databricks UI → Catalog Explorer → select table → Lineage tab
- [ ] C) Databricks UI → Compute → Cluster → Spark UI → SQL tab
- [ ] D) Databricks UI → SQL Editor → Query History → Table Insights

## Q7: Full refresh on Delta Live Tables
You are running a Delta Live Tables pipeline for the first time on a new Unity Catalog metastore. Stale checkpoints from a previous environment are causing incorrect state. You want to discard all checkpoints and reprocess every source record from scratch. Which DLT update type should you trigger?

- [ ] A) Triggered update
- [ ] B) Continuous update
- [ ] C) Full Refresh update
- [ ] D) Development mode update
