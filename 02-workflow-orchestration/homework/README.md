# Module 2: Homework Solutions 

1. 128.3 MiB
```text
Solution:
- Check from Kestra → Metrics → Value
- Result =  file.size 134,481,400
- The value is in bytes
- Size in KiB = 134,481,400 / 1024 = 131,329.51171875 KiB
- Size in MiB = 131,329.51171875 / 1024 = 128.2514762878418 MiB
```


2. green_tripdata_2020-04.csv
3. 24,648,499

Solution:
```sql
SELECT table_id, row_count
FROM `kestra-demo-497710.zoomcamp.__TABLES__` 
WHERE table_id LIKE 'yellow_tripdata_2020_%'
  AND table_id NOT LIKE '%ext%'

/*
BigQuery has a metadata table called __TABLES__ that stores row counts for every table
*/
```
From BQ, open in Google Sheets to get the total sum

```text
table_id	SUM of row_count
yellow_tripdata_2020_01	6405008
yellow_tripdata_2020_02	6299354
yellow_tripdata_2020_03	3007292
yellow_tripdata_2020_04	237993
yellow_tripdata_2020_05	348371
yellow_tripdata_2020_06	549760
yellow_tripdata_2020_07	800412
yellow_tripdata_2020_08	1007284
yellow_tripdata_2020_09	1341012
yellow_tripdata_2020_10	1681131
yellow_tripdata_2020_11	1508985
yellow_tripdata_2020_12	1461897
```
4. 1,734,051

Solution:
```sql
SELECT table_id, row_count
FROM `kestra-demo-497710.zoomcamp.__TABLES__` 
WHERE table_id LIKE 'green_tripdata_2020_%'
  AND table_id NOT LIKE '%ext%'
```

BQ → Google Sheets Pivot table
```text
table_id	SUM of row_count
green_tripdata_2020_01	447770
green_tripdata_2020_02	398632
green_tripdata_2020_03	223406
green_tripdata_2020_04	35612
green_tripdata_2020_05	57360
green_tripdata_2020_06	63109
green_tripdata_2020_07	72257
green_tripdata_2020_08	81063
green_tripdata_2020_09	87987
green_tripdata_2020_10	95120
green_tripdata_2020_11	88605
green_tripdata_2020_12	83130
Grand Total	1734051
```

5. 1925152
```csv
Row,	table_id,	row_count
1,	yellow_tripdata_2021_03,	1925152
```

6. Add a timezone property set to America/New_York in the Schedule trigger configuration

Ref: https://kestra.io/docs/workflow-components/triggers/schedule-trigger