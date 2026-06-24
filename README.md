# Data Engineering Zoomcamp 2026

# Overview

![Data Engineer Zoomcamp 2026 Overview](</img/Data Engineer Zoomcamp 2026 Overview.excalidraw.png>)

Personal coursework repository for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

The checklist below tracks my progress against the 2026 syllabus.

## Module 1: Containerization and Infrastructure as Code


- [x] Learn GCP fundamentals for the course environment
- [x] Understand Docker fundamentals and Docker Compose
- [x] Run PostgreSQL locally with Docker
- [x] Set up a local data engineering development environment
- [x] Use Terraform to provision cloud infrastructure
- [x] Complete Module 1 homework and assignments

## Module 2: Workflow Orchestration


- [x] Understand data lakes and workflow orchestration concepts
- [x] Build and schedule workflows with Kestra
- [x] Configure retries, backfills, and production-style pipeline mechanics
- [x] Load raw data into Google Cloud Storage (GCS)
- [x] Load structured data into BigQuery
- [x] Complete Module 2 homework and assignments

## Workshop 1: Data Ingestion


- [x] Learn data ingestion patterns with dlt
- [x] Read from APIs and handle scalable pipelines
- [x] Practice normalization and incremental loading
- [x] Complete the workshop homework

## Module 3: Data Warehousing


- [x] Understand BigQuery as a serverless data warehouse
- [x] Create external tables over files in GCS
- [x] Create native BigQuery tables from external data
- [x] Apply partitioning and clustering for query optimization
- [x] Learn BigQuery cost and performance best practices
- [x] Review BigQuery ML concepts
- [x] Complete Module 3 homework and assignments

## Module 4: Analytics Engineering

- [x] Understand analytics engineering and dimensional modeling
- [x] Set up dbt with DuckDB and BigQuery
- [x] Build staging models
- [x] Build intermediate models for reusable business logic
- [x] Build marts for analytics and reporting
- [x] Add dbt tests and documentation
- [x] Deploy dbt models and connect them to BI workflows
- [x] Complete Module 4 homework and assignments

## Module 5: Data Platforms


End-to-end Lakehouse on **Databricks**: Delta Lake + Unity Catalog + a medallion pipeline deployed as an Asset Bundle.

- [x] Understand the Lakehouse model (Delta Lake over object storage)
- [x] Build data pipelines with ~~Bruin~~ **Databricks**
- [x] Ingest raw data into a Bronze Delta table (`samples.nyctaxi.trips`)
- [x] Transform Bronze → Silver → Gold with Lakeflow Declarative Pipelines
- [x] Enforce data quality with pipeline expectations (`@dlt.expect_or_fail`)
- [x] Govern tables and track lineage with Unity Catalog
- [x] Deploy jobs and pipelines via Databricks Asset Bundles (`databricks bundle deploy`)
- [x] Complete Module 5 homework and assignments

## Module 6: Batch Processing

- [x] Learn Apache Spark fundamentals
- [x] Work with Spark DataFrames and SQL
- [x] Understand Spark internals for groupBy and joins
- [x] Process and analyze the NYC Taxi dataset with Spark
- [x] Complete Module 6 homework and assignments

## Module 7: Streaming


Real-time pipeline: **Producer (Python) → Kafka (Redpanda) → Flink → PostgreSQL**, using NYC taxi data.

- [x] Learn Kafka fundamentals (topics, offsets, consumer groups)
- [x] Run Redpanda as a drop-in, JVM-free Kafka replacement
- [x] Build Kafka producers and consumers in Python (`kafka-python`)
- [x] Stand up a Flink cluster and run PyFlink jobs (ARM64-compatible image)
- [x] Process event-time streams with watermarks and checkpointing
- [x] Build tumbling, sliding, and session windows in Flink SQL
- [x] Stream aggregations into PostgreSQL via the JDBC upsert sink
- [x] Complete Module 7 homework and assignments

## Final Project

This will based on my existing project [E-commerce-dashboard v1](https://github.com/jack2000-dev/e-commerce-dashboard) → E-commerce-dashboard v2

- [ ] Scope and design an end-to-end data pipeline
- [ ] Provision infrastructure with Terraform
- [ ] Ingest data from a public API or dataset
- [ ] Orchestrate the pipeline workflow with Apache Airflow
- [ ] Transform and model data in a warehouse
- [ ] Build a dashboard or analytical output
- [ ] Document the project and publish in Github repo

## References

- [Official Data Engineering Zoomcamp repository](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [2026 cohort registration and course materials](https://github.com/DataTalksClub/data-engineering-zoomcamp#how-to-enroll)
