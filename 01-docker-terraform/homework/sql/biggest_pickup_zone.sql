select
    t.Zone,
    sum(g.total_amount) as total_amount
from taxi_zone_lookup t
join green_tripdata_2025_11 g
on t.LocationID = g.PULocationID
where cast(g.lpep_pickup_datetime as DATE) = '2025-11-18'
group by 1
order by total_amount desc

-- Q5 Answer: East Harlem North with total_amount of 9281.919999999998