"""
Verify Question 1: What is the start date and end date of the dataset?

This script:
1. Runs the dlt pipeline to load data from the API into DuckDB
2. Queries the loaded data to find min/max dates
3. Also checks using the dlt MCP approach (execute_sql_query)

HOW TO VERIFY:
=============

METHOD 1 - Run this pipeline (requires internet):
    uv run python3 verify_q1.py

METHOD 2 - Use dlt MCP (after data is loaded):
    Ask the AI: "use dlt MCP execute_sql_query on taxi_pipeline:
    SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) 
    FROM taxi_dataset.taxi_data"

METHOD 3 - Query DuckDB directly (after data is loaded):
    uv run python3 -c "
    import duckdb
    con = duckdb.connect('taxi_pipeline.duckdb')
    print(con.sql('''
        SELECT 
            MIN(trip_pickup_date_time) as start_date,
            MAX(trip_pickup_date_time) as end_date
        FROM taxi_dataset.taxi_data
    ''').df())
    "

METHOD 4 - Use dlt CLI (after data is loaded):
    uv run dlt pipeline taxi_pipeline show
    Then browse the taxi_data table in the web UI
"""
import dlt
from dlt.sources.rest_api import rest_api_resources


@dlt.source
def taxi_source():
    """Define the NYC Taxi data REST API source."""
    config = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "paginator": "page_number",
                    "data_selector": "$[*]"
                }
            }
        ]
    }
    
    yield from rest_api_resources(config)


if __name__ == "__main__":
    import os
    
    # Delete old DuckDB to start fresh
    db_path = "taxi_pipeline.duckdb"
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed old {db_path}")
    
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_dataset",
        progress="log"
    )
    
    print("Loading data from API...")
    load_info = pipeline.run(taxi_source())
    print(load_info)
    
    # ===== VERIFY THE ANSWER =====
    import duckdb
    con = duckdb.connect(db_path)
    
    print("\n" + "=" * 60)
    print("QUESTION 1 VERIFICATION")
    print("=" * 60)
    
    print("\n--- Total rows ---")
    print(con.sql("SELECT COUNT(*) as total_rows FROM taxi_dataset.taxi_data").df().to_string(index=False))
    
    print("\n--- Date range (Trip_Pickup_DateTime) ---")
    result = con.sql("""
        SELECT 
            MIN(trip_pickup_date_time) as start_date, 
            MAX(trip_pickup_date_time) as end_date 
        FROM taxi_dataset.taxi_data
    """).df()
    print(result.to_string(index=False))
    
    print("\n--- Date range (Trip_Dropoff_DateTime) ---")
    result2 = con.sql("""
        SELECT 
            MIN(trip_dropoff_date_time) as start_date, 
            MAX(trip_dropoff_date_time) as end_date 
        FROM taxi_dataset.taxi_data
    """).df()
    print(result2.to_string(index=False))
    
    print("\n" + "=" * 60)
    print("ANSWER: The dataset spans from", 
          result.iloc[0]['start_date'], "to", result.iloc[0]['end_date'])
    print("=" * 60)
