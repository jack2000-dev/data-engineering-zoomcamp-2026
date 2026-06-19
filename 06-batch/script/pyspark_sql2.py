# Paramitizing script for Spark

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# 2. Initialize Spark with Shuffle Optimization
spark = SparkSession.builder \
    .appName('test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 3. Read and Prepare Green Taxi Data
df_green = spark.read.parquet(input_green)
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

# 4. Read and Prepare Yellow Taxi Data
df_yellow = spark.read.parquet(input_yellow)
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# 5. Define Common Columns
common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

# 6. Select and Align DataFrames
df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

# 7. Combine and Create Temp View
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.createOrReplaceTempView('trips_data')

# 8. Run Aggregation Query
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# 9. Coalesce and Write to GCS (Prevents memory crash)
df_result.coalesce(1).write.parquet(output, mode='overwrite')

# """
# spark-submit \
#     --master="spark://localhost:7077" \
#     pyspark_sql2.py \
#         --input_green=data/pq/green/2021/*/ \
#         --input_yellow=data/pq/yellow/2021/*/ \
#         --output=data/report/report-2021
# """