CREATE TABLE q5_session_window (
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (session_start, PULocationID)
);

/*
SELECT PULocationID, num_trips FROM q5_session_window ORDER BY num_trips DESC LIMIT 1;
*/