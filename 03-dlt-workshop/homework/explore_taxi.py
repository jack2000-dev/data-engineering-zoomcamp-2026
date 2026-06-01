import duckdb
import pandas as pd

# Connect to the DuckDB database for the taxi pipeline
con = duckdb.connect("taxi_pipeline.duckdb")

try:
    print("\n" + "="*40)
    print("1. TOTAL ROWS LOADED")
    print("="*40)
    print(con.sql("SELECT COUNT(*) as total_rows FROM taxi_dataset.taxi_data").df().to_string(index=False))

    print("\n" + "="*40)
    print("2. START AND END DATES (Based on Pickup Time)")
    print("="*40)
    print(con.sql("SELECT MIN(trip_pickup_date_time) as start_date, MAX(trip_pickup_date_time) as end_date FROM taxi_dataset.taxi_data").df().to_string(index=False))

except Exception as e:
    print(f"Error querying data: {e}")
    print("\nLet's check if the table exists...")
    print(con.sql("SHOW TABLES").df().to_string(index=False))
