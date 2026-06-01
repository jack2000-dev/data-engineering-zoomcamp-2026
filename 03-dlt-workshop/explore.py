import duckdb
import pandas as pd

# Connect to the DuckDB database created by dlt
con = duckdb.connect("open_library_pipeline.duckdb")

print("\n" + "="*40)
print("1. TABLES IN DATABASE")
print("="*40)
print(con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='open_library_data'").df().to_string(index=False))

print("\n" + "="*40)
print("2. SCHEMA FOR 'books' TABLE")
print("="*40)
print(con.sql("DESCRIBE open_library_data.books").df()[["column_name", "column_type"]].to_string(index=False))

print("\n" + "="*40)
print("3. SNEAK PEEK AT THE DATA (First 3 books)")
print("="*40)
print(con.sql("SELECT title, first_publish_year FROM open_library_data.books LIMIT 3").df().to_string(index=False))
print("\n")
