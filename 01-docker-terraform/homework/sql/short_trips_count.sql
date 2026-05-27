SELECT count(*) as trip_count
FROM green_tripdata_2025_11
where lpep_pickup_datetime >= '2025-11-01 00:00:00'
and lpep_pickup_datetime < '2025-12-01 00:00:00'
and trip_distance <= 1

-- Q3 answer: 8007