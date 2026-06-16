import urllib.request
from pyspark.sql.functions import lit


# 1. Create a Volume to stage the raw file (run once; safe to re-run)
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dev")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.dev.raw_files")

# 2. Define source URL and a Volume path to stage the file
source_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
volume_path = "/Volumes/workspace/dev/raw_files/yellow_tripdata_2024-01.parquet"

# 3. Download the file into the Volume
print(f"Downloading data from {source_url}...")
urllib.request.urlretrieve(source_url, volume_path)
print("Download complete.")

# 4. Read the staged file with Spark
df = spark.read.parquet(volume_path)

# 5. Add metadata
df = df.withColumn("ingestion_date", lit("2024-01"))

# 6. Write to a Unity Catalog Managed Table (Bronze layer)
df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("workspace.dev.bronze_yellow_taxi_raw")

print("✅ Data ingested and stored as table: workspace.dev.bronze_yellow_taxi_raw")