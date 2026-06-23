CREATE TABLE q6_largest_tip (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            num_trips BIGINT,
            total_tips DOUBLE PRECISION,
            total_revenue DOUBLE PRECISION,
            PRIMARY KEY (window_start)
);


-- SELECT window_start, total_tips FROM q6_largest_tip ORDER BY total_tips DESC LIMIT 1