# Module 4 Homework: Analytics Engineering with dbt

[Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/04-analytics-engineering/homework.md)

## Question 1. dbt Lineage and Execution

**Answer:** `int_trips_unioned` only

Note: 

In dbt, the `--select` (or -s) flag targets specific nodes in your project's Directed Acyclic Graph (DAG).

- Default Behavior: Specifying just the model name (--select int_trips_unioned) tells dbt to execute only that single file. It assumes the prerequisite staging models (stg_green_tripdata and stg_yellow_tripdata) have already been built in a prior run or already exist as tables/views in your warehouse.

- To include **upstream dependencies** (parents): You must place a plus sign before the model name: `dbt run --select +int_trips_unioned`

- To include **downstream dependencies** (children): You must place a plus sign after the model name: `dbt run --select int_trips_unioned+`

## Question 2. dbt Tests

**Answer:** dbt will fail the test, returning a non-zero exit code

The accepted_values test is a generic dbt test used to enforce data quality. Here is what happens behind the scenes:

- **The SQL Query:** When you run this test, dbt compiles and executes a SQL query that searches for any rows where payment_type is not in (1, 2, 3, 4, 5).

- **The Failure Trigger:** Because the new value 6 exists in the data, the query will return those rows. By default, if a dbt test query returns 1 or more rows, the test fails.

- **The Exit Code:** A failed test run stops the process and returns a non-zero exit code (typically 1). This is crucial because it signals deployment pipelines or orchestrators (like Airflow or Prefect) that the data has failed quality checks.

## Question 3. Counting Records in `fct_monthly_zone_revenue`

**Answer:** 12,184

```sql
select count(*) as rows_count
from `de-zoomcamp-2026-498303.dbt_prod.fct_monthly_zone_revenue`
```

## Question 4. Best Performing Zone for Green Taxis (2020)

**Answer:** East Harlem North

```sql
-- Solution
select 
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue
from `de-zoomcamp-2026-498303.dbt_prod.fct_monthly_zone_revenue` 
where service_type = 'Green'
  and revenue_month >= '2020-01-01' 
  and revenue_month < '2021-01-01'
group by 1
order by 2 desc
limit 5
```

## Question 5. Green Taxi Trip Counts (October 2019)

**Answer:** 384624

```sql
-- Solution
select sum(total_monthly_trips) as total_trip_count
from `de-zoomcamp-2026-498303.dbt_prod.fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month >= '2019-10-01'
  and revenue_month < '2019-11-01'
```

## Question 6. Build a Staging Model for FHV Data

**Answer:** 43244693 rows

```sql
-- Solution 
select count(*) as fhv_record_count
from `de-zoomcamp-2026-498303.dbt_prod.stg_fhv_tripdata`
```