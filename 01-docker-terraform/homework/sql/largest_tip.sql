select
    do_zone.Zone,
    max(g.tip_amount) -- Individual tip amount
from green_tripdata_2025_11 g
join taxi_zone_lookup pu -- Pickup zone
  on g.PULocationID = pu.LocationID
join taxi_zone_lookup do_zone -- drop off zone
  on g.DOLocationID = do_zone.LocationID
where g.lpep_pickup_datetime >= '2025-11-01'
  and g.lpep_pickup_datetime < '2025-12-01'
  and pu.Zone = 'East Harlem North'
group by 1
order by 2 desc




-- Q6 Answer: Yorkville West with tip_amount of 81.89