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

### Window
*What* to count into — a time bucket.
- **Tumbling** (`TUMBLE`) — fixed, non-overlapping (trips/hour)
- **Sliding/Hop** (`HOP`) — fixed, overlapping (moving averages, surge detection)
- **Session** (`SESSION`) — dynamic, closes after inactivity gap (user sessions)

### Watermark
*When* to publish a window. Trails the latest event by a tolerance (e.g., 5s). When the watermark passes a window's end, Flink emits that window. Without it, Flink would buffer forever.

### Checkpointing
Periodically snapshots Kafka offsets + in-flight state to disk. On crash, the job resumes from the last checkpoint. Trade-off: too frequent = expensive; too rare = more lost progress.

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
