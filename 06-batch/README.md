# Module 6 — Batch Processing with Apache Spark (Lecture Notes)

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

- **`.master("local[*]")`** — run locally using *all* cores (`local[4]` = 4 cores).
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

---

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
