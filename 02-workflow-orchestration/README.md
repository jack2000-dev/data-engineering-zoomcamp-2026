# Module 2: Workflow Orchestration with Kestra

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
# Reference
https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration