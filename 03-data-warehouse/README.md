# Module 3: Data Warehouse (BigQuery)

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

---

## Reference

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse
