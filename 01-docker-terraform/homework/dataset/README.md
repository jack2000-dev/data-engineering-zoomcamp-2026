# Prepare the Data

Download the green taxi trips data for November 2025:

```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
```
You will also need the dataset with zones:

```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```
For `wget` alternative, use `curl` (I rename the first file for convienience)

```
curl -o green_tripdata_2025_11.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
```

```
curl -o taxi_zone_lookup.csv https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```
