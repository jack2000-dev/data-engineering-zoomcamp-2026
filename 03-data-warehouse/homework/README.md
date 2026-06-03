## Question 1. Counting records
Answer: 20332093 count_rows

```sql
-- rows count
select count(*)
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`
```
## Question 2. Data read estimation

Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

```sql
select count (distinct PULocationID)
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024_external`
-- This query will process 0 B when run.

select count (distinct PULocationID)
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`

-- This query will process 155.12 MB when run.
```
Note: External table = pointer to GCS files.
0 B estimate = BigQuery does not estimate external table bytes the same way as native table bytes.

## Question 3. Understanding columnar storage

Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. Counting zero fare trips

Answer: 8,333

```sql
select count(fare_amount)
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`
where fare_amount = 0
```
## Question 5. Partitioning and clustering

Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
-- Create Partition and cluster table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024_part_cluster`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`;
```
Note: Parition is similary to group by where cluster is order by

## Question 6. Partition benefits

Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```sql
select distinct VendorID
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`
where tpep_dropoff_datetime > '2024-03-01'
  and tpep_dropoff_datetime < '2024-03-15'

-- This query will process 310.24 MB when run. (non-partitioned table)
```

```sql
select distinct VendorID
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024_part_cluster`
where tpep_dropoff_datetime > '2024-03-01'
  and tpep_dropoff_datetime < '2024-03-15'
  
-- This query will process 24.88 MB when run. (partitioned and clustered)
```

## Question 7. External table storage

Answer: GCP Bucket

Why?: The external table itself is a BigQuery object, but the actual data is still stored in your GCS bucket as parquet files.

`External table = BigQuery metadata/pointer | Actual data = GCS bucket`

## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data?

Answer: False

Why?: Clustering is useful only when your queries often filter, group, or order by the clustered columns. It is not always best practice.

Clustering may not help much when:

```text
- The table is small
- Queries do not filter on clustered columns
- The clustered column has low usefulness for pruning
- You already get enough benefit from partitioning
```

So the better rule is: cluster intentionally based on query patterns.

## Question 9. Understanding table scans

Answer: This query will process 0 B when run.

```sql
select count(*)
from `de-zoomcamp-2026-498303.ny_taxi.yellow_tripdata_2024`

-- This query will process 0 B when run.
```

Why?: `COUNT(*)` does not need to read any actual column data from a native BigQuery table. BigQuery can answer it from table metadata.
