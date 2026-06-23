CREATE TABLE processed_events_aggregated (
    window_start   TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    total_revenue  DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

-- Q4 query: SELECT PULocationID, num_trips FROM processed_events_aggregated ORDER BY num_trips DESC LIMIT 3;