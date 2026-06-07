# Module 4: Analytics Engineering with dbt

This module uses **dbt + BigQuery** to transform raw NYC taxi data into analytics-ready models.

## What is dbt?

**dbt** is a transformation tool for the **T in ELT**.

It does not extract data from APIs and it does not load files into the warehouse. Instead, dbt assumes raw data already exists in a warehouse like BigQuery, then runs SQL models to clean, join, test, and publish analytics tables.

In practice:

```text
Raw warehouse tables -> dbt SQL models -> clean analytics tables
```

dbt gives SQL projects software engineering structure:

- SQL models live in git as `.sql` files
- dependencies are inferred with `ref()` and `source()`
- Jinja templates make SQL reusable
- tests catch bad data before downstream tables are trusted
- seeds load small CSV reference tables
- lineage shows how tables depend on each other
- dbt Cloud runs the project against BigQuery

dbt does the orchestration inside the warehouse. BigQuery performs the actual compute.

---

## Flow

```text
DataTalksClub CSV.gz files
  -> Python upload script
  -> GCS bucket
  -> BigQuery raw tables in nytaxi
  -> dbt sources
  -> staging models
  -> intermediate models
  -> marts / fact tables
  -> analytics queries
```

Use the static DataTalksClub files, not the current NYC TLC files, because official TLC data can change over time.

---

## How dbt Is Used Here

This module uses dbt after the raw taxi files are already loaded into BigQuery.

1. Raw tables are created manually in BigQuery under `nytaxi`.
2. `sources.yml` tells dbt where those raw tables live.
3. Staging models standardize names and data types.
4. Intermediate models combine yellow and green trips.
5. Mart models create final tables for analysis.
6. dbt tests validate assumptions such as non-null IDs and accepted values.

The main project lives here:

```text
04-analytics-engineering/taxi_rides_ny
```

The important dbt files are:

| File / Folder | Purpose |
|---------------|---------|
| `dbt_project.yml` | Project config: model paths, materializations, profile name |
| `models/staging/sources.yml` | Defines raw BigQuery source tables in `nytaxi` |
| `models/staging/*.sql` | Cleans and renames raw taxi columns |
| `models/intermediate/*.sql` | Combines reusable model logic |
| `models/marts/*.sql` | Final analytics tables |
| `seeds/*.csv` | Small lookup tables committed to git |
| `macros/*.sql` | Reusable Jinja/SQL helpers |
| `packages.yml` | External dbt packages |

---

## Key Concepts

| Concept | Meaning |
|---------|---------|
| **Source** | Existing raw table in the warehouse, defined in `sources.yml` |
| **Staging model** | Cleans raw data: casts types, renames columns, filters bad rows |
| **Intermediate model** | Combines or reshapes staging models for reuse |
| **Mart** | Final analytics table used by users or reports |
| **Seed** | Small CSV tracked in git and loaded by dbt, e.g. taxi zones |
| **Test** | Data quality check; failing tests return a non-zero exit code |
| **Lineage** | dbt DAG showing dependencies between sources, models, and marts |

Model layers in this project:

```text
models/staging       -> stg_green_tripdata, stg_yellow_tripdata, stg_fhv_tripdata
models/intermediate  -> int_trips_unioned, int_trips
models/marts         -> fct_trips, dim_zones, dim_vendors
models/marts/reporting -> fct_monthly_zone_revenue
```

---

## 1. Upload Raw Files to GCS

> [!NOTE]
> This is a local development pattern. The JSON key stays outside git and is passed through `GOOGLE_APPLICATION_CREDENTIALS`. In production, avoid long-lived JSON keys when possible. Prefer attaching a service account to the runtime, or use Workload Identity / Secret Manager depending on where the pipeline runs.


Set credentials and bucket:

```bash
cd 04-analytics-engineering

export GOOGLE_APPLICATION_CREDENTIALS="../keys/your-service-account.json"
export GCS_BUCKET_NAME="de_zoomcamp_2026_demo"
```


Install dependency if needed:

```bash
uv add google-cloud-storage
```

Upload yellow and green taxi data for 2019-2020:

```bash
uv run python load_taxi_data.py
```

Upload FHV data for 2019:

```bash
TAXI_TYPES=fhv uv run python load_taxi_data.py
```

Expected GCS layout:

```text
gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/yellow/
gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/green/
gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/fhv/
```
---

## 2. Create Raw BigQuery Tables

Create or use the raw dataset:

```text
nytaxi
```

Load from GCS in BigQuery UI:

| Table | GCS URI |
|-------|---------|
| `nytaxi.yellow_tripdata` | `gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/yellow/yellow_tripdata_*.csv.gz` |
| `nytaxi.green_tripdata` | `gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/green/green_tripdata_*.csv.gz` |
| `nytaxi.fhv_tripdata` | `gs://de_zoomcamp_2026_demo/04-analytics-engineering/raw/fhv/fhv_tripdata_*.csv.gz` |

BigQuery load settings:

```text
Source format: CSV
Header rows to skip: 1
Auto detect: enabled
```

Verify:

```sql
select count(*) from `PROJECT_ID.nytaxi.yellow_tripdata`;
select count(*) from `PROJECT_ID.nytaxi.green_tripdata`;
select count(*) from `PROJECT_ID.nytaxi.fhv_tripdata`;
```

---

## 3. dbt Cloud Setup

Use this repo in dbt Cloud and set the project subdirectory:

```text
04-analytics-engineering/taxi_rides_ny
```

BigQuery connection:

```text
Dataset / schema: dbt_prod
Location: same as nytaxi dataset, e.g. US
Auth: service account JSON
```

Useful service account roles for the course:

```text
BigQuery Data Editor
BigQuery Job User
BigQuery User
BigQuery Read Session User
```

Environment variable:

```text
DBT_GCP_PROJECT_ID=your-gcp-project-id
```

`models/staging/sources.yml` uses this variable to find:

```text
PROJECT_ID.nytaxi.yellow_tripdata
PROJECT_ID.nytaxi.green_tripdata
PROJECT_ID.nytaxi.fhv_tripdata
```

Never commit `profiles.yml`; it can contain private keys.

---

## 4. Run dbt

In dbt Cloud IDE:

```bash
dbt deps
dbt debug
dbt build
```

`dbt build` runs seeds, models, and tests.

If using a local dbt profile with a `prod` target:

```bash
dbt build --target prod
```

Expected BigQuery outputs depend on the dbt Cloud schema configuration. In this setup, models were built under:

```text
dbt_prod
```

Important tables:

```text
dbt_prod.fct_trips
dbt_prod.fct_monthly_zone_revenue
dbt_prod.stg_fhv_tripdata
```

---

## dbt Tests and Documentation

dbt tests are SQL checks defined mostly in `schema.yml` files. A test passes when it returns **zero rows**. If it returns rows, dbt treats those rows as failures.

Common generic tests:

| Test | What it checks |
|------|----------------|
| `not_null` | Column should not contain null values |
| `unique` | Column values should be unique |
| `accepted_values` | Column should only contain allowed values |
| `relationships` | Foreign key values should exist in another model |

Example from this project:

```yaml
columns:
  - name: service_type
    data_tests:
      - accepted_values:
          arguments:
            values: ['Green', 'Yellow']
      - not_null
```

Run tests only:

```bash
dbt test
```

Run one model and its tests:

```bash
dbt build --select fct_trips
```

dbt documentation comes from:

- model descriptions in `schema.yml`
- column descriptions in `schema.yml`
- source definitions in `sources.yml`
- lineage from `ref()` and `source()`

Generate docs:

```bash
dbt docs generate
```

In dbt Cloud, use the **Lineage** and documentation views in the IDE. Locally, if dbt is installed, serve generated docs with:

```bash
dbt docs serve
```

The docs site is useful for answering:

- What does this model do?
- What columns does it produce?
- Which upstream sources/models does it depend on?
- Which downstream models will break if this changes?

---

## Common Commands

Run one model only:

```bash
dbt run --select int_trips_unioned
```

Run upstream dependencies too:

```bash
dbt run --select +int_trips_unioned
```

Run downstream dependencies too:

```bash
dbt run --select int_trips_unioned+
```

Run tests:

```bash
dbt test
```

Build one model and its dependencies:

```bash
dbt build --select +fct_monthly_zone_revenue
```

---

## Troubleshooting Notes

If dbt cannot find `dbt_project.yml`, check the dbt Cloud project subdirectory:

```text
04-analytics-engineering/taxi_rides_ny
```

If dbt looks for `target:database.nytaxi`, fix `sources.yml`:

```yaml
database: "{{ env_var('DBT_GCP_PROJECT_ID') }}"
schema: nytaxi
```

If `--target prod` fails with `target 'prod' not found`, run:

```bash
dbt build
```

If dbt warns about `numeric` precision/scale, it is safe for this homework. The models still build.

If dbt Cloud shows `profiles.yml` in version control, discard it. Do not commit credentials.

---

## Reference

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering
