-- Calculate differences between updates for given trips and vehicles
CREATE OR REPLACE TABLE positions AS
    SELECT p.*,
        COALESCE(
            timestamp - LAG(timestamp) OVER (
                PARTITION BY route_id, trip_id, vehicle_id 
                ORDER BY timestamp
            ),
            INTERVAL '1 second'
        ) AS time_diff
    FROM positions p;

-- Create a local trip_id that is uniuqe for each trip inside a
-- given (trip_id, vehicle_id)
-- NOTE: this won't produce the same hashes for logically corresponding trips if
--       they are inserted incrementally  
CREATE OR REPLACE TABLE positions AS
    SELECT p.*,
        SUM(
            CASE WHEN time_diff > INTERVAL '5 minutes' THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY route_id, trip_id, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS local_trip_id
    FROM positions p;

-- Create a global_trip_id that is unique for each trip no matter what
CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (time_diff, local_trip_id),
        hash(route_id, trip_id, vehicle_id, local_trip_id) AS global_trip_id
    FROM positions p;