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
