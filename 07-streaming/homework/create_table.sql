CREATE TABLE green_trips (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_datetime VARCHAR,
    dropoff_datetime VARCHAR
);

-- Query for Q3: select count(*) from green_trips where trip_distance > 5