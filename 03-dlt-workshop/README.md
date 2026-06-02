# Module 3: dlt Workshop

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

---

## Homework

Answers and verification scripts are in [`homework/README.md`](homework/README.md).
