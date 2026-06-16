from pyspark.sql.functions import lit


# 1. Ensure the target catalog + schema exist (safe to re-run)
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dev")

# 2. Read from the built-in sample dataset.
#    Serverless compute has no public internet egress, so instead of downloading
#    the NYC parquet from cloudfront we use the sample table shipped with the
#    workspace (no network required).
df = spark.read.table("samples.nyctaxi.trips")

# 3. Add metadata
df = df.withColumn("ingestion_date", lit("2024-01"))

# 4. Write to a Unity Catalog Managed Table (Bronze layer)
df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("workspace.dev.bronze_yellow_taxi_raw")

print("✅ Data ingested and stored as table: workspace.dev.bronze_yellow_taxi_raw")
