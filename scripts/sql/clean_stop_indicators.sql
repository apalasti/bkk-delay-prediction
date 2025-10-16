CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (
            current_stop_sequence, 
            stop_id
        ),
        IF(
            current_stop_sequence IS NULL,
            LEAD(current_stop_sequence) OVER (
                PARTITION BY global_trip_id
                ORDER BY timestamp
            ),
            current_stop_sequence
        ) AS current_stop_sequence,
        IF(
            current_stop_sequence IS NULL,
            LEAD(stop_id) OVER (
                PARTITION BY global_trip_id
                ORDER BY timestamp
            ),
            stop_id
        ) AS stop_id,
    FROM positions p;

CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (
            current_stop_sequence, 
            stop_id
        ),
        coalesce(max(CASE WHEN stop_id IS NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY global_trip_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::BOOLEAN AS had_null_stop, -- Signifies that the trip has been completed
        IF(had_null_stop, NULL, current_stop_sequence) AS current_stop_sequence,
        IF(had_null_stop, NULL, stop_id) AS stop_id,
    FROM positions p;

CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (
            had_null_stop,
            current_stop_sequence,
            stop_id,
        ),
        min(current_stop_sequence) OVER (
            PARTITION BY p.global_trip_id
            ORDER BY timestamp
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS current_stop_sequence,
        argmin(stop_id, current_stop_sequence) OVER (
            PARTITION BY p.global_trip_id
            ORDER BY timestamp
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS stop_id
    FROM positions p;