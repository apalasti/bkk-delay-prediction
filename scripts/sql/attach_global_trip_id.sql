CREATE OR REPLACE TABLE positions AS
    SELECT p.*,
        timediff('second', st.departure_time, strftime(p.timestamp, '%H:%M:%S')::TIME) AS diff_from_start,
        p.timestamp - INTERVAL (diff_from_start) SECOND AS scheduled_start,
        strftime(scheduled_start , '%Y-%m-%d') || '_' || p.trip_id || '_' || p.vehicle_id AS global_trip_id
    FROM positions p
    JOIN stop_times st ON st.trip_id = p.trip_id AND st.stop_sequence = 0;


-- Remove trips where the stop sequence regresses (i.e., bus goes backwards on the route)
CREATE OR REPLACE TABLE positions AS
    WITH anomalies AS (
        SELECT DISTINCT global_trip_id
        FROM (
            SELECT
                p.global_trip_id,
                p.current_stop_sequence,
                LAG(p.current_stop_sequence) OVER (PARTITION BY p.global_trip_id ORDER BY p.timestamp) AS prev_stop_sequence,
                p.timestamp,
                LAG(p.timestamp) OVER (PARTITION BY p.global_trip_id ORDER BY p.timestamp) AS prev_timestamp
            FROM positions p
        ) seq
        WHERE 
            (seq.prev_stop_sequence IS NOT NULL AND seq.current_stop_sequence < seq.prev_stop_sequence)
            OR
            (seq.prev_timestamp IS NOT NULL AND seq.timestamp - seq.prev_timestamp > INTERVAL '20 minutes')
    )
    SELECT p.* EXCLUDE (diff_from_start, scheduled_start)
    FROM positions p
    WHERE p.global_trip_id NOT IN (SELECT global_trip_id FROM anomalies);
