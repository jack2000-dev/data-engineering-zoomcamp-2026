select
    lpep_pickup_datetime,
    trip_distance
from green_tripdata_2025_11
where trip_distance < 100
order by trip_distance desc
limit 1

-- Q4 Answer: 2025-11-14