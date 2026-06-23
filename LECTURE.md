# Data Engineering Zoomcamp 2026 — Lecture Notes

> My single-source notes for the [DataTalksClub Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).
> Each module is grounded in the code and homework under its matching folder in this repo.

![DE Zoomcamp 2026 Overview](img/Data%20Engineer%20Zoomcamp%202026%20Overview.excalidraw.png)

## Contents

| # | Module | Topic | Stack |
| --- | --- | --- | --- |
| 1 | [Module 1](#module-1--docker--terraform) | Containerization & IaC | Docker, Terraform, Postgres |
| 2 | [Module 2](#module-2--workflow-orchestration-kestra) | Workflow Orchestration | Kestra, GCS, BigQuery |
| 3 | [Module 3](#module-3--data-warehouse-bigquery) | Data Warehouse | BigQuery |
| 3.5 | [Module 3 Workshop](#module-3-workshop--data-loading-with-dlt) | Data Loading | dlt, DuckDB |
| 4 | [Module 4](#module-4--analytics-engineering-dbt) | Analytics Engineering | dbt, BigQuery |
| 5 | [Module 5](#module-5--data-platforms-databricks) | Data Platforms | Databricks, Spark, Delta Lake |
| 6 | [Module 6](#module-6--batch-processing-apache-spark) | Batch Processing | Apache Spark, PySpark |
| 7 | [Module 7](#module-7--stream-processing-pyflink) | Stream Processing | Kafka/Redpanda, PyFlink |

---

# Module 1 — Docker & Terraform

## Docker Structure
![Docker Structure](img/Docker-Structure-for-DE-Zoomcamp-2026.excalidraw.png)

---

## What is Docker?

A platform to package applications into **containers** — isolated, reproducible environments
that run the same everywhere (your laptop, server, cloud).

**Why it matters for data engineering:**
- No "works on my machine" problems
- Easy to spin up databases, tools, and pipelines
- Reproducible environments for ETL jobs

---

## Key Docker Concepts

| Concept | What it is |
|---------|------------|
| **Image** | A blueprint/template (like a class) |
| **Container** | A running instance of an image (like an object) |
| **Dockerfile** | Instructions to build an image |
| **Volume** | Persistent storage that survives container restarts |
| **Port mapping** | `host:container` — exposes container ports to your machine |
| **Network** | Containers in the same `docker-compose` share a network automatically |

---

## Dockerfile Anatomy

```dockerfile
FROM python:3.13.11-slim              # Base image

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/   # Multi-stage: copy uv binary

WORKDIR /app                          # Set working directory

ENV PATH="/app/.venv/bin:$PATH"       # Add venv to PATH

COPY pyproject.toml uv.lock ./        # Copy deps first (cache layer)
RUN uv sync --locked                  # Install dependencies

COPY ingest_data.py .                 # Copy application code

ENTRYPOINT ["python", "ingest_data.py"]  # Default command
```

**Layer caching tip:** Copy dependency files before source code.
If only your code changes, Docker reuses the cached dependency layer.

---

## Docker Compose

Defines and runs **multi-container** applications in a single YAML file.

```yaml
services:
  pgdatabase:                    # Service name = hostname on the network
    image: postgres:18
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "ny_taxi_postgres_data:/var/lib/postgresql:rw"
    ports:
      - "5432:5432"              # host:container

  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8085:80"

volumes:
  ny_taxi_postgres_data:         # Named volume for data persistence
```

**Key commands:**

```bash
docker compose up          # Start all services
docker compose up -d       # Start in background (detached)
docker compose down        # Stop and remove containers
docker compose ps          # List running containers
docker compose logs -f     # Follow logs
```

**Networking:** Containers reference each other by **service name** (e.g., `pgdatabase` as hostname), not `localhost`.

---

## Data Ingestion Pipeline

The pipeline downloads NYC taxi CSV data and loads it into Postgres in chunks.

```
CSV (from GitHub) → pandas (chunked read) → PostgreSQL
```

**Key pattern: chunked ingestion**

```python
df_iter = pd.read_csv(url, iterator=True, chunksize=100000)

for df_chunk in df_iter:
    df_chunk.to_sql(name=table, con=engine, if_exists='append')
```

Why chunks? The full CSV is too large to fit in memory at once.

**Running the ingestion container:**

```bash
docker build -t taxi_ingest .

docker run --network=pipeline_default taxi_ingest \
  --pg-host pgdatabase \
  --pg-user root \
  --pg-pass root \
  --pg-db ny_taxi \
  --year 2021 \
  --month 1
```

> Note: `--network` must match the docker-compose network so the container can reach `pgdatabase`.

---

## Terraform

Infrastructure as Code (IaC) — define cloud resources in `.tf` files instead of clicking through the console.

### Why Terraform?

- **Reproducible** — same config = same infrastructure every time
- **Version controlled** — track infrastructure changes in git
- **Multi-cloud** — works with GCP, AWS, Azure, etc.

### File Structure

| File | Purpose |
|------|---------|
| `main.tf` | Resource definitions (what to create) |
| `variables.tf` | Input variables (configurable values) |
| `terraform.tfstate` | Current state of infrastructure (auto-generated, don't edit) |

### main.tf Breakdown

```hcl
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)    # Service account key
  project     = var.project
  region      = var.region
}

# GCS Bucket
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_storage_class
}

# BigQuery Dataset
resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
```

### Key Commands

```bash
terraform init      # Download provider plugins
terraform plan      # Preview what will be created/changed
terraform apply     # Create/update resources
terraform destroy   # Delete all resources
```

### Workflow

```
terraform init  →  terraform plan  →  terraform apply
   (once)          (review changes)    (create resources)
```

> ⚠️ Always run `terraform plan` before `apply` to review changes.
> ⚠️ Don't forget `terraform destroy` when done to avoid GCP charges.

---

## GCP Resources Created

| Resource | Terraform Resource Type | Purpose |
|----------|------------------------|---------|
| GCS Bucket | `google_storage_bucket` | Store raw data files |
| BigQuery Dataset | `google_bigquery_dataset` | Data warehouse for querying |

---

# Module 2 — Workflow Orchestration (Kestra)

## Handling GCP Credentials in Kestra

Kestra uses `{{ secret('GCP_CREDS') }}` to access GCP service account credentials in flows.
The way you provide this secret differs between **local development** and **production**.

---

### How Kestra Secrets Work

| Function | Source | Use Case |
|----------|--------|----------|
| `{{ secret('KEY') }}` | Environment variable `SECRET_KEY` (base64-encoded) | Sensitive data (API keys, credentials) |
| `{{ kv('KEY') }}` | KV Store (Kestra UI → Namespace → KV Store) | Non-sensitive config (project ID, bucket name) |

Kestra OSS reads secrets from environment variables prefixed with `SECRET_`.
The value must be **base64-encoded**.

---

### Local Development (Docker Compose + `.env`)

Best approach: store secrets in a `.env` file, reference them in `docker-compose.yaml`.

#### Step 1: Create the service account key

```bash
# In GCP Console:
# IAM & Admin → Service Accounts → Create Key → JSON
# Download the JSON file
```

#### Step 2: Base64-encode the key

```bash
cat keys/your-service-account.json | base64
```

#### Step 3: Store in `.env`

```env
# .env (same directory as docker-compose.yaml)
SECRET_GCP_CREDS={long ass string}
```

> ⚠️ **Never commit `.env` to git.** Make sure `.env` is in your `.gitignore`.

#### Step 4: Reference in `docker-compose.yaml`

```yaml
services:
  kestra:
    image: kestra/kestra:v1.1
    environment:
      SECRET_GCP_CREDS: ${SECRET_GCP_CREDS}
      KESTRA_CONFIGURATION: |
        # ... rest of config
```

Docker Compose automatically reads `.env` files in the same directory and substitutes `${SECRET_GCP_CREDS}` with the value.

#### Step 5: Use in Kestra flows

```yaml
tasks:
  - id: gcp_task
    type: io.kestra.plugin.gcp.bigquery.Query
    serviceAccount: "{{ secret('GCP_CREDS') }}"
    # Kestra strips the SECRET_ prefix and base64-decodes automatically
```

#### How the chain works

```
.env file                    → docker-compose.yaml           → Kestra container env     → Flow
SECRET_GCP_CREDS=<base64>    → ${SECRET_GCP_CREDS}           → SECRET_GCP_CREDS=<b64>   → {{ secret('GCP_CREDS') }}
```

---

### Production Approaches

In production, you should **never** store secrets in files or environment variables directly.

#### Option 1: Kestra Enterprise — Secrets UI

```
Kestra UI → Namespaces → your-namespace → Secrets → Add Secret
```

- Encrypted at rest
- Managed through the UI or API
- Requires Kestra Enterprise Edition

#### Option 2: External Secret Manager (Recommended)

Use a cloud-native secret manager and configure Kestra to read from it.

**Google Secret Manager:**

```yaml
# In KESTRA_CONFIGURATION
kestra:
  secret:
    type: gcp-secret-manager
    gcp-secret-manager:
      project: your-gcp-project-id
      # Kestra reads secrets directly from GCP Secret Manager
```

**AWS Secrets Manager:**

```yaml
kestra:
  secret:
    type: aws-secret-manager
    aws-secret-manager:
      region: us-east-1
```

**HashiCorp Vault:**

```yaml
kestra:
  secret:
    type: vault
    vault:
      address: https://vault.example.com
      token: your-vault-token
```

#### Option 3: Kubernetes Secrets

If running Kestra on Kubernetes, mount secrets as environment variables:

```yaml
# Kubernetes deployment
env:
  - name: SECRET_GCP_CREDS
    valueFrom:
      secretKeyRef:
        name: kestra-secrets
        key: gcp-creds
```

---

### Quick Reference

| Environment | Method | Secret Storage |
|-------------|--------|----------------|
| **Local dev** | `.env` + `docker-compose.yaml` | `.env` file (git-ignored) |
| **Production (simple)** | Kestra Enterprise Secrets UI | Kestra internal (encrypted) |
| **Production (best)** | External secret manager | GCP Secret Manager / Vault / AWS |
| **Kubernetes** | K8s Secrets | Cluster secret store |

---

### Security Checklist

- [ ] `.env` is in `.gitignore`
- [ ] `keys/` directory is in `.gitignore`
- [ ] Service account has **minimum required roles** (not Owner)
- [ ] Rotate keys periodically
- [ ] Never paste secrets in Kestra flow YAML directly

---

## What is Workflow Orchestration?

Coordinating and managing a sequence of tasks (a **pipeline**) that extract, transform, and load data.
Without orchestration, you'd run scripts manually and hope nothing breaks.

**Kestra** is the orchestrator in this module — it handles scheduling, retries, dependencies, and monitoring.

---

## Kestra Core Concepts

| Concept | What it is |
|---------|------------|
| **Flow** | A YAML file defining a pipeline (tasks + triggers + inputs) |
| **Task** | A single unit of work (download file, run SQL, upload to GCS) |
| **Trigger** | What starts a flow (schedule, manual, webhook) |
| **Namespace** | Logical grouping for flows (e.g., `zoomcamp`) |
| **Execution** | A single run of a flow |
| **KV Store** | Key-value storage for config (project ID, bucket name) |
| **Secrets** | Encrypted values for credentials (`SECRET_` env vars) |

---

## Flow Anatomy

```yaml
id: my_flow
namespace: zoomcamp

inputs:                    # User-provided parameters
  - id: taxi
    type: SELECT
    values: [yellow, green]

tasks:                     # Steps executed in order
  - id: extract
    type: io.kestra.plugin.core.http.Download
    uri: "https://..."

  - id: load_to_gcs
    type: io.kestra.plugin.gcp.gcs.Upload
    serviceAccount: "{{ secret('GCP_CREDS') }}"

  - id: load_to_bq
    type: io.kestra.plugin.gcp.bigquery.Query
    serviceAccount: "{{ secret('GCP_CREDS') }}"

triggers:                  # When to run
  - id: monthly_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"    # 1st of every month at 10:00
```

---

## ETL Pipeline Pattern (This Module)

```
Source (NYC TLC)  →  Kestra  →  GCS Bucket  →  BigQuery
     CSV/Parquet      Extract     Stage         Load
                      ↓           ↓             ↓
                    Download    Upload file    Create external table
                    from URL    to bucket      + native table
```

Each monthly run creates two BigQuery tables:
- `yellow_tripdata_2021_01_ext` → external table (points to GCS file)
- `yellow_tripdata_2021_01` → native table (data copied into BQ storage)

---

## Scheduling & Backfill

### Schedule Trigger

```yaml
triggers:
  - id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"          # Runs 1st of each month
    timezone: America/New_York   # Optional: set timezone
    inputs:
      taxi: yellow
```

**Cron cheat sheet:** `minute hour day-of-month month day-of-week`

| Cron | Meaning |
|------|---------|
| `0 10 1 * *` | 1st of every month at 10:00 |
| `0 0 * * *` | Every day at midnight |
| `*/5 * * * *` | Every 5 minutes |

### Backfill

Retroactively run a scheduled flow for **past dates**.

- Go to flow → **Triggers** tab → click **Backfill**
- Set start/end date range
- Kestra creates one execution per schedule interval
- `{{ trigger.date }}` is populated for each simulated date

> ⚠️ `{{ trigger.date }}` only works with schedule triggers or backfill — not manual execution.

---

## Useful BigQuery Queries

**Row counts from metadata (no data scan):**
```sql
SELECT table_id, row_count
FROM `project.dataset.__TABLES__`
WHERE table_id LIKE 'yellow_tripdata_2020_%'
  AND table_id NOT LIKE '%ext%'
```

**Total rows across tables:**
```sql
SELECT SUM(row_count) as total
FROM `project.dataset.__TABLES__`
WHERE table_id LIKE 'yellow_tripdata_2020_%'
  AND table_id NOT LIKE '%ext%'
```

> 💡 Wildcard table queries (`table_*`) don't work when external tables match the pattern.

---

## Docker Compose Setup

This module runs 4 containers:

| Container | Port | Purpose |
|-----------|------|---------|
| `kestra` | 8080, 8081 | Workflow orchestrator UI + API |
| `kestra_postgres` | — | Kestra internal database |
| `pgdatabase` | 5432 | NY Taxi data (local Postgres) |
| `pgadmin` | 8085 | Database admin UI |

```bash
docker compose up       # Start all containers
docker compose down     # Stop and remove containers
```

---

# Module 3 — Data Warehouse (BigQuery)

## What is a Data Warehouse?

A centralized repository optimized for **analytical queries** (OLAP), not transactions (OLTP).

| | OLTP (Postgres) | OLAP (BigQuery) |
|---|---|---|
| **Purpose** | Day-to-day operations | Analytics & reporting |
| **Queries** | Small, frequent reads/writes | Large scans, aggregations |
| **Design** | Normalized (3NF) | Denormalized (star/snowflake) |
| **Examples** | PostgreSQL, MySQL | BigQuery, Snowflake, Redshift |

---

## BigQuery Overview

Google's **serverless** data warehouse — no infrastructure to manage.

**Key features:**
- No servers to provision or manage
- Pay per query (bytes scanned) + storage
- Columnar storage format
- Built-in caching and auto-optimization
- Separates compute and storage

---

## Table Types in BigQuery

| Type | Storage | Cost | Speed | Use Case |
|------|---------|------|-------|----------|
| **Native (Managed)** | Data stored in BQ | Storage + query cost | Fast | Production queries |
| **External** | Data stays in GCS/Drive | Query cost only | Slower | Staging, raw data |

### External Table

Points to files in GCS without copying data into BigQuery.

```sql
CREATE OR REPLACE EXTERNAL TABLE `project.dataset.green_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/green_tripdata_2022-*.parquet']
);
```

### Native Table (from External)

```sql
CREATE OR REPLACE TABLE `project.dataset.green_tripdata`
AS SELECT * FROM `project.dataset.green_tripdata_ext`;
```

---

## Partitioning

Divides a table into **segments** based on a column value (usually a date).
Queries that filter on the partition column only scan relevant partitions → **less data scanned = lower cost**.

```sql
CREATE OR REPLACE TABLE `project.dataset.trips_partitioned`
PARTITION BY DATE(tpep_pickup_datetime)
AS SELECT * FROM `project.dataset.green_tripdata`;
```

### How it works

```
┌─────────────────────────────────┐
│         trips_partitioned       │
├─────────┬─────────┬─────────────┤
│ 2020-01 │ 2020-02 │ 2020-03 ... │   ← each partition = one date segment
└─────────┴─────────┴─────────────┘
```

**Query with partition filter:**
```sql
-- Only scans the January partition (~50 MB)
SELECT * FROM trips_partitioned
WHERE tpep_pickup_datetime BETWEEN '2020-01-01' AND '2020-01-31';
```

**Without partition filter:**
```sql
-- Scans ALL partitions (~1.5 GB)
SELECT * FROM trips_partitioned;
```

### Partition types

| Strategy | Column Type | Example |
|----------|-------------|---------|
| `DATE` | DATE/TIMESTAMP | `PARTITION BY DATE(pickup_datetime)` |
| `RANGE` | INTEGER | `PARTITION BY RANGE_BUCKET(id, ...)` |
| Ingestion time | Auto | `PARTITION BY _PARTITIONTIME` |

> 💡 Best for columns used in `WHERE` filters with **high cardinality dates**.
> Limit: max **4,000 partitions** per table.

---

## Clustering

Sorts data **within each partition** (or the whole table) by one or more columns.
Improves performance for queries filtering/ordering on those columns.

```sql
CREATE OR REPLACE TABLE `project.dataset.trips_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `project.dataset.green_tripdata`;
```

### How it works

```
┌───────────────── Partition: 2020-01 ─────────────────┐
│  VendorID=1  │  VendorID=1  │  VendorID=2  │ VendorID=2 │  ← sorted by cluster key
└──────────────┴──────────────┴──────────────┴────────────┘
```

### Partitioning vs Clustering

| Feature | Partitioning | Clustering |
|---------|-------------|------------|
| **Based on** | Single column | Up to 4 columns |
| **How** | Physically separates data | Sorts within partitions |
| **Cost estimate** | Known before query | Estimated |
| **Best for** | Date-based filters | High-cardinality filters |
| **Limit** | 4,000 partitions | No limit |

### When to use what

| Scenario | Recommendation |
|----------|---------------|
| Filter by date | Partition by date |
| Filter by category + date | Partition by date, cluster by category |
| Small table (< 1 GB) | Neither (overhead not worth it) |
| Very high cardinality filter (e.g., user_id) | Cluster (too many unique values for partitions) |

---

## BigQuery Best Practices

### Cost optimization

- **Avoid `SELECT *`** — only select columns you need (columnar storage)
- **Use partitioned/clustered tables** — reduces bytes scanned
- **Preview with `LIMIT`** won't save cost — BQ still scans full columns
- **Check bytes scanned** — shown in top-right before running a query
- **Use `__TABLES__`** for row counts instead of `COUNT(*)`:
  ```sql
  SELECT table_id, row_count
  FROM `project.dataset.__TABLES__`
  ```

### Query performance

- **Filter on partition columns** in `WHERE` clauses
- **Filter on cluster columns** after partition filters
- **Avoid excessive JOINs** — denormalize when possible
- **Use approximate functions** — `APPROX_COUNT_DISTINCT()` is faster than `COUNT(DISTINCT)`

---

## Credential Management

Use Google client libraries through **Application Default Credentials (ADC)** instead of hardcoding credential paths in Python code.

### Local development

For local homework work, keep the service account JSON key outside Git and pass it through the environment:

```bash
cd 03-data-warehouse
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/../keys/your-key-file.json"
export GCS_BUCKET_NAME="your-unique-bucket-name"
uv run python load_yellow_taxi_data.py
```

Then initialize the GCS client without referencing the JSON file directly:

```python
BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
client = storage.Client()
```

Why this is preferred:

- No personal paths such as `/Users/...` in source code
- No secrets or credential filenames embedded in Python
- Same code works across laptops, CI, and cloud runtimes
- Credentials can be rotated without editing application code

Keep credential files out of Git:

```gitignore
*.json
.env
```

### Production

In production, avoid service account JSON keys when possible. Attach a service account directly to the workload that runs the code:

| Runtime | Production credential pattern |
|---------|-------------------------------|
| Compute Engine | Attach the service account to the VM |
| Cloud Run | Set the service account on the Cloud Run service |
| GKE | Use Workload Identity |
| Cloud Composer / Airflow | Use the environment or worker service account |
| GitHub Actions or external CI | Use Workload Identity Federation instead of storing JSON keys |

The Python code stays the same:

```python
client = storage.Client()
```

Google Cloud automatically provides short-lived credentials to the runtime through the attached service account. This is safer than JSON keys because there is no long-lived private key file to leak, copy, or commit.

For this homework, the practical roles are:

| Role | Scope | Why |
|------|-------|-----|
| `roles/storage.admin` | Project or bucket setup phase | Create/check the bucket and upload parquet files |
| `roles/storage.objectAdmin` | Existing bucket | Less broad option if the bucket already exists |
| `roles/bigquery.jobUser` | Project | Run BigQuery jobs and queries |
| `roles/bigquery.dataEditor` | Dataset | Create external, native, partitioned, and clustered tables |

Use the narrowest scope that works. For a learning project, project-level roles are convenient. For production, prefer bucket-level and dataset-level grants.

---

## Homework BigQuery Setup

After the six Yellow Taxi parquet files are uploaded to GCS, create the BigQuery tables manually in **BigQuery Studio > SQL editor**. Do not use the **Load data** button for this homework.

Expected GCS files:

```text
gs://your-bucket/yellow_tripdata_2024-01.parquet
gs://your-bucket/yellow_tripdata_2024-02.parquet
gs://your-bucket/yellow_tripdata_2024-03.parquet
gs://your-bucket/yellow_tripdata_2024-04.parquet
gs://your-bucket/yellow_tripdata_2024-05.parquet
gs://your-bucket/yellow_tripdata_2024-06.parquet
```

Use these placeholders in the SQL below:

| Placeholder | Meaning |
|-------------|---------|
| `PROJECT_ID` | Your Google Cloud project ID |
| `DATASET` | BigQuery dataset name, for example `ny_taxi` |
| `BUCKET_NAME` | GCS bucket containing the parquet files |

Create a BigQuery dataset if needed:

```sql
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.DATASET`;
```

Create an external table that reads the parquet files directly from GCS:

```sql
CREATE OR REPLACE EXTERNAL TABLE `PROJECT_ID.DATASET.yellow_tripdata_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://BUCKET_NAME/yellow_tripdata_2024-*.parquet']
);
```

The external table does not copy data into BigQuery storage. It points BigQuery at the parquet files in GCS.

Create a regular native BigQuery table from the external table. Do not partition or cluster this table:

```sql
CREATE OR REPLACE TABLE `PROJECT_ID.DATASET.yellow_tripdata_2024`
AS
SELECT *
FROM `PROJECT_ID.DATASET.yellow_tripdata_2024_external`;
```

Verify both tables have the same row count:

```sql
SELECT COUNT(*) AS record_count
FROM `PROJECT_ID.DATASET.yellow_tripdata_2024_external`;

SELECT COUNT(*) AS record_count
FROM `PROJECT_ID.DATASET.yellow_tripdata_2024`;
```

The expected record count for January-June 2024 Yellow Taxi data is:

```text
20,332,093
```

Flow summary:

```text
GCS parquet files
  -> external table reads files in place
  -> regular table copies data into native BigQuery storage
```

Use a dataset location compatible with the GCS bucket location. Keeping both in the same region or compatible multi-region avoids location errors.

---

## BigQuery Internals (How It Works)

### Columnar Storage

Data stored **by column**, not by row. Querying specific columns only reads those columns.

```
Row-based:     [id, name, date, amount] [id, name, date, amount] ...
Columnar:      [id, id, id, ...] [name, name, name, ...] [date, date, ...] [amount, amount, ...]
                                                                ↑
                                                    Only this column read if you
                                                    SELECT date FROM table
```

### Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   Dremel     │     │   Colossus   │     │   Jupiter    │
│  (compute)   │────▶│  (storage)   │◀────│  (network)   │
│  query exec  │     │  columnar    │     │  petabit/s   │
└─────────────┘     └──────────────┘     └──────────────┘
```

- **Dremel** — Execution engine (breaks queries into stages, runs in parallel)
- **Colossus** — Distributed storage (columnar format, automatic compression)
- **Jupiter** — Network (connects compute to storage at high speed)

---

## BigQuery ML (Bonus)

Build ML models directly in BigQuery using SQL — no need to export data.

```sql
-- Create a linear regression model
CREATE OR REPLACE MODEL `project.dataset.tip_model`
OPTIONS (model_type='linear_reg', input_label_cols=['tip_amount']) AS
SELECT passenger_count, trip_distance, fare_amount, tip_amount
FROM `project.dataset.trips`;

-- Predict
SELECT * FROM ML.PREDICT(MODEL `project.dataset.tip_model`,
  (SELECT passenger_count, trip_distance, fare_amount
   FROM `project.dataset.new_trips`));
```

## Reference

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse

---

# Module 3 Workshop — Data Loading with dlt

## What is dlt?

`dlt` is a Python library for building data loading pipelines. It handles extraction,
schema inference, normalization, state, and loading into destinations like DuckDB,
BigQuery, Postgres, and files.

**Why it matters for data engineering:**
- Less boilerplate than hand-written ETL
- Automatic schema creation and evolution
- Incremental loading support
- Easy local development with DuckDB

---

## Setup

This module uses `uv` and Python 3.13:

```bash
cd 03-dlt-workshop
uv sync
```

Key files:

| File | What it does |
|------|--------------|
| `open_library_pipeline.py` | Defines the API source and runs the dlt pipeline |
| `explore.py` | Connects to DuckDB and previews tables/data |
| `open_library_pipeline.duckdb` | Local database created by dlt |

---

## Pipeline Pattern

This example loads book search results from the Open Library API into DuckDB.

```text
Open Library API -> dlt REST source -> DuckDB
     JSON docs      schema/load        open_library_pipeline.duckdb
```

The source is defined with `rest_api_resources`:

| Setting | Meaning |
|---------|---------|
| `base_url` | API root |
| `resources` | Tables/streams to extract |
| `data_selector` | JSON path that contains records |
| `params` | Query string sent to the API |
| `paginator` | Pagination strategy |

---

## Running the Pipeline

```bash
uv run python open_library_pipeline.py
```

This calls `https://openlibrary.org/search.json`, extracts records from `docs`,
and loads them into `open_library_data.books`.

---

## Inspecting the Data

```bash
uv run python explore.py
```

This prints:
- tables in the `open_library_data` schema
- the `books` table schema
- a small preview of book titles and publish years

Example query:

```sql
SELECT title, first_publish_year
FROM open_library_data.books
LIMIT 3;
```

---

## Key dlt Concepts

| Concept | What it is |
|---------|------------|
| **Source** | Group of resources from one data source |
| **Resource** | One extractable table/stream |
| **Pipeline** | dlt object that runs extract/load work |
| **Destination** | Where data is loaded, such as DuckDB |
| **State** | Stored metadata for incremental runs |

## Homework

Answers and verification scripts are in [`03-dlt-workshop/homework/README.md`](03-dlt-workshop/homework/README.md).

## Reference
- https://colab.research.google.com/github/anair123/data-engineering-zoomcamp/blob/workshop/dlt_2026/cohorts/2026/workshops/dlt/dlt_Pipeline_Overview.ipynb#scrollTo=-kNiY112Xvuk

---

# Module 4 — Analytics Engineering (dbt)

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

## Reference

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering

---

# Module 5 — Data Platforms (Databricks)

> DE Zoomcamp 2026, Module 5 (Databricks edition). This is the condensed module
> overview. The **full, detailed lecture** lives in
> [`05-data-platforms/homework/LECTURE.md`](05-data-platforms/homework/LECTURE.md); the hands-on bundle is under
> [`05-data-platforms/databricks_taxi_pipeline/`](05-data-platforms/databricks_taxi_pipeline/), and the self-test is
> [`05-data-platforms/homework/homework.md`](05-data-platforms/homework/homework.md).

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
→ full reasoning in [`05-data-platforms/homework/LECTURE.md`](05-data-platforms/homework/LECTURE.md).

---

# Module 6 — Batch Processing (Apache Spark)

> DE Zoomcamp 2026, Module 6. These notes are grounded in the scripts under
> `script/` and the notebooks under `notebook/`. Read top-to-bottom once, then use
> the cheat sheet at the end as a reference.

---

## 0. The big picture: why Spark / batch processing

Two ways to process data:

- **Batch** — process a large, *bounded* chunk at a fixed cadence (hourly/daily/monthly).
  Simple, easy to retry, easy to reason about. This module.
- **Streaming** — process events continuously as they arrive (next module's territory).

**Apache Spark** is a distributed compute engine for batch (and streaming). You write
PySpark/SQL and Spark splits the work into **partitions** and runs them in **parallel
across executors**. You reach for Spark when the data is too big for pandas/one machine,
or when you need to express transformations that don't fit cleanly in SQL.

Rule of thumb from the course: if the data fits in a data warehouse and SQL does the job,
use SQL (BigQuery/dbt). Drop to Spark when you need code-level flexibility (UDFs, ML,
complex logic) or when orchestrating heavy file-based transforms.

---

## 1. The SparkSession — your entry point

Everything starts with a `SparkSession`. Importing `pyspark` alone does **not** create it.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
```

- **`.master("local[*]")`** — run locally using *all* cores (`local` = 4 cores).
  In a real cluster this becomes `spark://<host>:7077` (standalone) or `yarn`.
- **`.appName(...)`** — name shown in the Spark UI.
- **`.getOrCreate()`** — reuse an existing session or build one.

See `script/test_spark.py` for the minimal smoke test (`spark.version`, `spark.range(10).show()`).

### 1.1 The Spark UI (Homework Q5)
While a session is alive, Spark serves a dashboard at **`http://localhost:4040`** showing
jobs, stages, tasks, and the DAG. If 4040 is taken it rolls to 4041, 4042, …

> ⚠️ **4040 is the UI, not the cluster master.** The master's RPC port is **7077**.
> Pointing `--master` at 4040 produces the cryptic `Too large frame: …` error — the
> driver speaks RPC to an HTTP port.

---

## 2. Reading data & schemas

```python
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("data/raw/yellow/2025/file.csv")
```

### 2.1 Why define a schema (don't trust `inferSchema`)
CSV has no types. Spark options:

- **`inferSchema=true`** — Spark reads the file *twice* (slow) and often guesses
  everything as `string`, or picks `LongType`/`DoubleType` when you wanted `Integer`.
- **Explicit `StructType`** — fast (one pass), correct types, smaller Parquet output.

The pattern used in `notebook/pyspark.ipynb`: sample the head, let pandas infer, then
hand-write a clean Spark schema:

```python
!head -n 1001 data/raw/.../file.csv > head.csv          # sample
df_pandas = pd.read_csv("head.csv")
spark.createDataFrame(df_pandas).schema                  # see Spark's guess

schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
    ...
])
```

Full hand-built schemas for the taxi data live in `script/green_taxi_data.py` and
`script/yellow_taxi_data.py`. Note green uses `lpep_*` datetime columns, yellow uses
`tpep_*` — this matters in §5.

### 2.2 Parquet remembers its schema
Parquet is **columnar + typed + compressed**. Reading it back needs no schema and no
header option — the types are baked in:

```python
df = spark.read.parquet("data/pq/green/2021/01/")
df.printSchema()   # types preserved
```

This is why the pipeline converts raw CSV.gz → Parquet once, then everything downstream
reads Parquet.

---

## 3. Partitions & repartition (Homework Q2)

A Spark DataFrame is split into **partitions** — the unit of parallelism. One partition =
one task = one core at a time. One giant `.csv.gz` is a *single* partition (gzip isn't
splittable), so only one core works ⇒ no parallelism.

**`repartition(N)`** reshuffles the data into N roughly-equal partitions, which then write
as **N part files**:

```python
df.repartition(4).write.parquet(output_path, mode="overwrite")
```

`notebook/repartition.ipynb` demonstrates the homework: repartition Yellow Nov-2025 to 4,
write, and the part files come out ~24 MB each (avg ≈ 25 MB):

```python
import glob, os
parts = glob.glob("data/processed/yellow/2025/11/part-*.parquet")
avg_mb = sum(os.path.getsize(p) for p in parts) / len(parts) / 1024 / 1024
```

### 3.1 repartition vs coalesce
| | Shuffle? | Can increase partitions? | Use when |
|---|---|---|---|
| `repartition(N)` | **Yes** (full shuffle) | Yes | rebalance / parallelize |
| `coalesce(N)` | No (merges) | No, only **decrease** | collapse to fewer files cheaply |

`script/pyspark_sql2.py` ends with `df_result.coalesce(1)` — the aggregated report is tiny,
so one output file is fine and a shuffle would be wasteful.

### 3.2 Write modes
`mode="overwrite"` replaces the output dir; the default errors if the path exists
(`PATH_ALREADY_EXISTS`). Always set it when re-running a pipeline step.

---

## 4. Transformations vs Actions (lazy evaluation)

Spark is **lazy**. Transformations build a plan; nothing runs until an **action** triggers it.

- **Transformations** (lazy): `select`, `filter`, `withColumn`, `groupBy`, `join`,
  `repartition`, `withColumnRenamed`, `union`.
- **Actions** (trigger execution): `show()`, `count()`, `collect()`, `write...`, `take()`.

This is why `spark.sql("…")` alone shows nothing in a notebook — it returns a DataFrame
(a plan); you need an action like `.show()`. Laziness lets Spark optimize the whole chain
before running it.

### 4.1 DataFrame API
```python
df.select("pickup_datetime", "PULocationID") \
  .filter(df.hvfhs_license_num == "HV0003") \
  .show()

df.withColumn("pickup_date", F.to_date(df.pickup_datetime))   # add/derive columns
df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
```

`from pyspark.sql import functions as F` is the standard import for built-ins
(`F.lit`, `F.to_date`, `F.col`, `F.max`, `F.count`, `unix_timestamp`, …).

### 4.2 User-Defined Functions (UDFs)
When logic is too awkward for SQL, wrap a Python function as a UDF (`notebook/pyspark.ipynb`):

```python
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:  return f's/{num:03x}'
    elif num % 3 == 0: return f'a/{num:03x}'
    else:              return f'e/{num:03x}'

crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
df.withColumn("base_id", crazy_stuff_udf(df.dispatching_base_num))
```

UDFs are flexible but slower than native Spark functions (data crosses the JVM↔Python
boundary, and the optimizer can't see inside them) — prefer built-ins when possible.

---

## 5. Spark SQL — querying DataFrames with SQL

Register a DataFrame as a temp view, then query it:

```python
df.createOrReplaceTempView("trips")     # modern API
# df.registerTempTable("trips")         # older, deprecated alias

spark.sql("""
    select count(*) as count
    from trips
    where date(tpep_pickup_datetime) = '2025-11-15'
""").show()
```

`notebook/pyspark_sql_hw.ipynb` answers the homework both ways (DataFrame API **and** SQL):

- **Q3 — trips on Nov 15:** `df.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()` → **162,604**
- **Q4 — longest trip (hours):** cast both timestamps to `long` (epoch seconds), subtract,
  `/3600`, take `max` → **90.6 h**
- **Q6 — least frequent pickup zone:** join trips to `taxi_zone_lookup.csv` on
  `PULocationID = LocationID`, group by `Zone`, order ascending →
  *Governor's Island / Arden Heights / Rikers Island* (all 1–4 trips)

### 5.1 The unify-and-aggregate pattern
`script/pyspark_sql2.py` is the canonical "report" job — combine green + yellow into one
schema and aggregate monthly revenue per zone:

1. Read both Parquet datasets.
2. Rename `lpep_*`/`tpep_*` → common `pickup_datetime` / `dropoff_datetime`.
3. `.select(common_columns)` and tag each with `F.lit('green')` / `F.lit('yellow')` as `service_type`.
4. `df_green_sel.unionAll(df_yellow_sel)` → one DataFrame.
5. `createOrReplaceTempView('trips_data')` → run the `GROUP BY zone, month, service_type`.
6. `.coalesce(1).write.parquet(output, mode='overwrite')`.

`.config("spark.sql.shuffle.partitions", "4")` lowers the default 200 shuffle partitions —
sensible for local/small data so you don't get 200 tiny files.

---

## 6. Running jobs: `spark-submit` & parameterization

Notebooks are for prototyping; production logic goes into a `.py` file submitted with
`spark-submit`. Parameterize with `argparse` so the same script runs any month
(`script/pyspark_sql2.py`):

```python
parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
```

```bash
spark-submit \
    --master="spark://localhost:7077" \
    pyspark_sql2.py \
        --input_green="data/pq/green/2021/*/" \
        --input_yellow="data/pq/yellow/2021/*/" \
        --output=data/report/report-2021
```

> 🐚 **zsh gotcha:** quote glob args (`"data/pq/green/2021/*/"`). Unquoted, zsh tries to
> expand the `*` itself and aborts with `no matches found` if the path doesn't exist —
> the wildcard is meant for Spark, not the shell.

When **not** connecting to a standalone cluster, drop `--master` and Spark runs locally.

---

## 7. Spark + the cloud (GCS & BigQuery)

The local pipeline (CSV → Parquet → report) ports to the cloud with minimal changes.

### 7.1 Upload Parquet to GCS
```bash
gcloud storage cp -r data/pq/green  gs://de_zoomcamp_2026_demo/pq/
gcloud storage cp -r data/pq/yellow gs://de_zoomcamp_2026_demo/pq/
```
(Set the active project first: `gcloud config set project de-zoomcamp-2026-498303`.)
From there a Spark job can read `gs://…` paths directly instead of local `data/…`.

### 7.2 Write results to BigQuery
`script/pyspark_sql_bq.py` is identical to the report job except the sink:

```python
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-...')   # staging area for the BQ connector
df_result.write.format('bigquery').option('table', output).save()
```

This is what you'd run on **Dataproc** (GCP's managed Spark) — Spark reads Parquet from GCS,
aggregates, and lands the result table in BigQuery.

---

## 8. The end-to-end flow in this module

```
raw CSV.gz (download_data.sh)
      │  spark.read.csv + explicit schema
      ▼
green_taxi_data.py / yellow_taxi_data.py
      │  repartition(4) → write.parquet(mode="overwrite")
      ▼
data/pq/{green,yellow}/YEAR/MONTH/        ← columnar, typed, parallel-friendly
      │  read parquet, rename to common cols, unionAll, GROUP BY
      ▼
pyspark_sql2.py  → data/report/...        (local Parquet)
pyspark_sql_bq.py → BigQuery table        (cloud / Dataproc)
```

---

## 9. Cheat sheet

**Session**
```python
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.version          # 4.1.2
```

**Read / write**
```python
spark.read.option("header","true").schema(s).csv(path)   # CSV needs schema+header
spark.read.parquet(path)                                 # Parquet self-describing
df.repartition(4).write.parquet(out, mode="overwrite")   # N part files
df.coalesce(1).write.parquet(out, mode="overwrite")      # 1 file, no shuffle
```

**Transform**
```python
df.select(...).filter(df.col == x)
df.withColumn("new", F.to_date(df.ts))
df.withColumnRenamed("old","new")
green.unionAll(yellow)
F.udf(fn, returnType=types.StringType())
```

**SQL**
```python
df.createOrReplaceTempView("t");  spark.sql("select ... from t").show()
```

**Submit**
```bash
spark-submit --master spark://localhost:7077 job.py --input "data/pq/*/" --output out
```

**Gotchas**
- Spark is **lazy** — `spark.sql(...)` returns a DataFrame; add `.show()`/`.count()` to run it.
- **Port 4040 = UI, 7077 = master RPC.** `Too large frame` = pointed `--master` at the UI.
- `PATH_ALREADY_EXISTS` ⇒ add `mode="overwrite"`.
- gzip CSV = 1 partition (not splittable) ⇒ `repartition` to parallelize.
- **Quote glob args** in zsh or it errors `no matches found` before Spark sees them.
- `curl -o <file> <url>` — output filename comes *right after* `-o`, then the URL.

**Homework answers:** Q1=4.1.2 · Q2=25 MB · Q3=162,604 · Q4=90.6 h · Q5=4040 ·
Q6=Governor's Island / Arden Heights / Rikers Island
*(see `homework/homework.md`).*

## Appendix — operational snippets

### Upload data from local to GCS
```bash
gcloud storage cp -r data/pq/green  gs://de_zoomcamp_2026_demo/pq/
gcloud storage cp -r data/pq/yellow gs://de_zoomcamp_2026_demo/pq/
```

### Download raw taxi data
```bash
bash script/download_data.sh yellow 2020     # loops all 12 months
```

---

# Module 7 — Stream Processing (PyFlink)

Real-time pipeline: **Producer (Python) → Kafka (Redpanda) → Flink → PostgreSQL**, using NYC taxi data.

## Big picture: batch vs streaming

| | Batch (Module 6) | Streaming (this module) |
|---|---|---|
| Data | bounded chunk (a file, a month) | unbounded, never-ending event feed |
| Trigger | scheduled (hourly/daily) | continuous, event-by-event |
| Job lifetime | runs and exits | runs 24/7 like a server |
| Latency | minutes–hours | sub-second to seconds |

**When to use streaming:** only when an *automated process reacts in real time* (fraud detection, surge pricing). If a human just looks at a dashboard, micro-batch (every 15 min / hourly) is simpler and cheaper. A streaming job that breaks at 3 AM needs someone on-call.

## The mindset shift (read this first)

If you're confused, it's almost always because streaming breaks two habits you built with batch/SQL:

1. **The job never finishes.** A batch script reads a file, computes, prints, and exits. A streaming job is a *server*: you start it and it runs forever, waiting for the next event. When your Flink job "hangs" and doesn't return to the prompt — that's not a bug, that's the whole idea. You read the results from Postgres while it's still running, then cancel it from the UI.

2. **Results come out in chunks, not all at once.** You don't get a final answer; you get a stream of partial answers as time advances. A "window" is how you chop the never-ending stream into finite pieces you *can* answer (e.g. "trips per 5 minutes").

**One analogy for the whole module:** think of a sushi conveyor belt.
- The **producer** is the chef putting plates on the belt (one plate = one taxi trip event).
- **Kafka/Redpanda** is the belt itself — it just carries plates in order and remembers them for a while.
- **Flink** is you at the counter, grouping plates into batches ("every 5 minutes of plates, count them").
- **Postgres** is your notebook where you write down each batch's total.

The chef never stops, so neither do you — you just keep tallying batch after batch.

## Architecture

```
Producer (Python)  ──JSON──▶  Kafka / Redpanda  ──▶  Flink  ──JDBC──▶  PostgreSQL
   reads taxi parquet          message broker        windows/agg      results table
   sends rows as events        (topic = rides)
```

## Components

### 1. Model (`src/models.py`)
A `@dataclass` defining the event schema — the shared contract between producer and consumer.
```python
@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int   # epoch MILLISECONDS — the format Flink expects
```
- `ride_from_row(row)` — converts a pandas row → `Ride`
- `ride_deserializer(data)` — converts Kafka bytes → `Ride` (bytes → str → dict → Ride)

### 2. Producer (`src/producers/producer.py`)
Reads parquet, sends each row as a JSON message to the `rides` topic.
- Connects to `localhost:9092` (external address, for your laptop)
- `value_serializer` converts `Ride` → JSON bytes automatically
- `producer.send()` only **queues**; `producer.flush()` forces delivery
- `time.sleep(0.01)` simulates real-time pacing

### 3. Consumer (`src/consumers/consumer.py`)
Reads messages from Kafka and prints them.
- `auto_offset_reset='earliest'` — new group reads from message #0
- `group_id` — Kafka's bookmark. Same group = resume; new group = restart
- `value_deserializer` converts bytes → `Ride` automatically

### 4. Consumer → Postgres (`src/consumers/consumer_postgres.py`)
Same consumer loop, but writes each ride to a Postgres table via `psycopg2`.
- Uses `cur.execute(sql, (params,))` with `%s` placeholders (SQL-injection safe)
- `conn.autocommit = True` — save every INSERT immediately
- Different `group_id` from the console consumer (independent offset tracking)

### 5. Redpanda (Kafka-compatible broker)
- Drop-in Kafka replacement (same protocol, `kafka-python` works unchanged)
- Single C++ binary: no JVM, no ZooKeeper
- **Two listeners** (this trips people up):
  - `redpanda:29092` — internal Docker address (used by Flink container)
  - `localhost:9092` — external address (used by your Python scripts)

### 6. Flink cluster
- **JobManager** — coordinator (UI at `localhost:8081`, accepts jobs, manages checkpoints)
- **TaskManager** — worker (runs the actual processing in **task slots**)
- Custom Docker image (`Dockerfile.flink`): Flink + Python 3.12 + PyFlink + Kafka/JDBC JARs
- **Checkpointing** (`enable_checkpointing(10*1000)`) — snapshots state every 10s; on crash, resumes from last checkpoint instead of restarting

## Flink jobs (`src/job/`)

### Pass-through (`pass_through_job.py`)
Kafka → Postgres, no transformation. Declares two tables in Flink SQL:
- **Source table** (`events`) — a virtual view over the Kafka `rides` topic
- **Sink table** (`processed_events`) — a JDBC connection to Postgres
- `INSERT INTO sink SELECT ... FROM source` moves the data

### Aggregation (`aggregation_job.py`)
1-hour tumbling window: count trips + sum revenue per pickup location.

**The trio that makes windowing work:**
1. **Event time** — `event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3)` — which field defines "when" an event happened
2. **Watermark** — `WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND` — how long to wait for late events before publishing a window. Trails the latest event by 5s.
3. **Upsert (PRIMARY KEY)** — `PRIMARY KEY (window_start, PULocationID) NOT ENFORCED` on the sink — lets Flink send corrections if a late event arrives after the window was published

```sql
INSERT INTO processed_events_aggregated
SELECT window_start, PULocationID,
       COUNT(*) AS num_trips, SUM(total_amount) AS total_revenue
FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
)
GROUP BY window_start, PULocationID;
```

## Key concepts

### Topic
A named, ordered, append-only mailbox for messages. Producer writes to it, consumer reads from it. Messages stay for days (retention), so multiple consumers can read independently.

### Offset
A message's position number in a topic (0, 1, 2, …). Each consumer group tracks its own offset — its "bookmark."

### Consumer group
A named team of consumers. Kafka tracks each group's offset separately. Same `group_id` restarted = resume; different `group_id` = re-read from start.

### Event time vs processing time (the concept that confuses everyone)

Two different clocks exist, and mixing them up causes wrong answers:

- **Event time** — *when the trip actually happened*, baked into the data (`lpep_pickup_datetime`). This is what you almost always want.
- **Processing time** — *when Flink happened to read the message* (wall-clock). Depends on network speed, replays, when you started the job — not reproducible.

Why it matters: messages can arrive **late or out of order**. Imagine trips A (8:04) and B (8:06). Due to a network hiccup, Flink reads B *before* A. If you bucketed by processing time, A would land in the wrong window. By using **event time**, both go to the correct 8:00–8:05 / 8:05–8:10 windows regardless of arrival order. That's why the source DDL builds `event_timestamp` from the data and declares a `WATERMARK` on it.

### Window
*What* to count into — a way to chop an infinite stream into finite buckets.

- **Tumbling** (`TUMBLE`) — fixed, non-overlapping. "Trips per 5 minutes." (Q4)
  ```
  events: · ··  ·   ·· ···   ·  ··
          |--------|--------|--------|
          8:00     8:05     8:10     8:15
           win 1    win 2    win 3        every event lands in exactly ONE window
  ```
- **Sliding / Hop** (`HOP`) — fixed size, overlapping. "5-min count, recomputed every 1 min." (moving averages)
  ```
  |-----|
     |-----|
        |-----|     an event can belong to SEVERAL windows
  ```
- **Session** (`SESSION`) — dynamic, *per key*. A window grows while events keep arriving within a gap; it closes after a gap of inactivity. (Q5)
  ```
  PULoc 42:  · · · ·          · ·              ·
             |-------|        |---|           |-|
             session A        sess B          C
              ↑ 4 trips close together   ↑ >5min gap starts a new session
  ```

### Watermark (why your window sometimes shows nothing)

A watermark is Flink's way of saying *"I'm now confident I've seen every event up to time T — so any window ending at or before T can be finalized and published."*

It **trails the latest event** by your tolerance. With `event_timestamp - INTERVAL '5' SECOND`, the watermark is always 5s behind the newest event seen. Concretely:

```
newest event seen: 8:05:30   →   watermark = 8:05:25
→ window 8:00–8:05 is now safe to emit (its end 8:05 ≤ 8:05:25)
→ a straggler from 8:04 arriving now is still counted (within tolerance)
→ a straggler older than the tolerance is dropped (or corrected via the upsert sink)
```

Two consequences that bite beginners:
- **No watermark progress = no output, ever.** If the watermark can't advance, no window's end is ever "passed," so Flink buffers forever and your Postgres table stays empty. (See the troubleshooting section — this is the #1 cause of "my job runs but nothing appears.")
- **The very last window may never fire.** With a bounded dataset, the final events have nothing after them to push the watermark past their window's end. That window just stays open. This is normal and almost never affects the answer.

### Checkpointing
Periodically snapshots Kafka offsets + in-flight state to disk. On crash, the job resumes from the last checkpoint. Trade-off: too frequent = expensive; too rare = more lost progress.

## Flink SQL tables are NOT real tables

This trips up SQL people: in a Flink job, `CREATE TABLE` does **not** create storage. It declares a *connector* — a live pipe to something external:

- A **source** table (`'connector' = 'kafka'`) is a continuously-updating view over a Kafka topic. Reading from it never "ends."
- A **sink** table (`'connector' = 'jdbc'`) is a write-pipe to a Postgres table that **must already exist** (Flink won't create it for you).

So the workflow is always: create the real Postgres table yourself → declare a matching Flink sink table → `INSERT INTO sink SELECT ... FROM source`. The column names/types on both sides must line up, or the job fails at planning.

## Lecture code vs your homework (don't mix them up)

The lecture/workshop code and the homework use **different data and conventions**:

| | Lecture (`src/`) | Homework (`homework/`) |
|---|---|---|
| Data | yellow taxi | green taxi |
| Topic | `rides` | `green-trips` |
| Timestamp field | `tpep_pickup_datetime` | `lpep_pickup_datetime` |
| Timestamp format | epoch **milliseconds** (int) | datetime **string** `'2025-10-01 00:21:47'` |
| Parse in DDL | `TO_TIMESTAMP_LTZ(field, 3)` | `TO_TIMESTAMP(field, 'yyyy-MM-dd HH:mm:ss')` |

The two `TO_TIMESTAMP` functions are **not** interchangeable: `TO_TIMESTAMP_LTZ(x, 3)` expects a number (epoch ms); `TO_TIMESTAMP(x, format)` expects a string. Using the wrong one yields `NULL` → the watermark never advances → no output.

## Troubleshooting: "my job runs but Postgres stays empty"

Streaming failures are usually silent (a job that produces nothing looks the same as a job that's "still working"). Check these in order:

1. **Parallelism > number of partitions.** The `green-trips` topic has **1 partition**. If `set_parallelism()` is higher, the extra subtasks sit idle, hold their watermark at −∞, and `min(all watermarks)` never advances → no window ever fires. **Fix:** `env.set_parallelism(1)`.
2. **Timestamps parse to NULL.** If your `TO_TIMESTAMP(...)` format doesn't match the actual string (or you used `_LTZ` on a string / `TO_TIMESTAMP` on a number), `event_timestamp` is NULL and the watermark is stuck. **Check:** `docker exec -it 07-streaming-redpanda-1 rpk topic consume green-trips --num 1` and confirm the timestamp is the shape your DDL expects.
3. **The sink Postgres table doesn't exist, or types mismatch.** The JDBC sink won't auto-create. A `DOUBLE` expression into a `BIGINT` column also fails. **Check:** the Flink UI (http://localhost:8081) → the failed job → exceptions tab for the real error. (A `try/except` that just `print`s will hide this from your terminal.)
4. **You re-sent data and have duplicates.** Delete + recreate the topic, then re-produce:
   `docker exec -it 07-streaming-redpanda-1 rpk topic delete green-trips`
5. **It's not actually broken — it's streaming.** The job runs forever by design. Give it 1–2 minutes, query Postgres *while it runs*, then cancel it from the Flink UI.

> Container names: this folder is `07-streaming`, so containers are `07-streaming-redpanda-1`, `07-streaming-jobmanager-1`, `07-streaming-postgres-1`. (Course docs say `workshop-…` — substitute your prefix.)

## Submitting a Flink job

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/<job>.py --pyFiles /opt/src -d
```

## ARM64 (Apple Silicon) note

The course Dockerfile assumes amd64. On ARM64, `pemja` (a PyFlink C extension) has no prebuilt wheel and must compile from source, requiring:
- Full JDK (not just JRE) — for JNI headers
- `build-essential` + `python3-dev` — for the C compiler
- Symlink `/opt/java/openjdk` → actual JDK path (pemja hardcodes this path)

See `Dockerfile.flink` for the working ARM64 setup.

## Cleanup

```bash
docker compose down        # stop containers
docker compose down -v     # also drop the Postgres volume
```

## Offset cleanup

```bash
docker compose exec redpanda rpk topic delete green-trips
docker compose exec redpanda rpk topic create green-trips
```

## File structure

```
07-streaming/
├── docker-compose.yaml          # redpanda + postgres + jobmanager + taskmanager
├── Dockerfile.flink             # custom Flink image (ARM64-compatible)
├── flink-config.yaml            # Flink cluster config
├── pyproject.flink.toml         # PyFlink dependencies (copied as pyproject.toml in image)
└── src/
    ├── models.py                # Ride dataclass + serializer/deserializer
    ├── producers/
    │   └── producer.py          # KafkaProducer → 'rides' topic
    ├── consumers/
    │   ├── consumer.py          # KafkaConsumer → console
    │   └── consumer_postgres.py # KafkaConsumer → Postgres
    └── job/
        ├── pass_through_job.py  # Kafka → Postgres (no transformation)
        └── aggregation_job.py   # 1-hour tumble, watermarks, upsert sink
```