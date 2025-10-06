-- Custom function for calculating the shortest distance between two time points
-- STRICTLY time points
CREATE OR REPLACE MACRO timediff(part, start_t, end_t) AS
CASE
    WHEN 12 < datediff('hour', start_t, end_t) 
        THEN -(datediff(part, end_t, TIME '23:59:59') + datediff(part, TIME '00:00:00', start_t))
    WHEN datediff('hour', start_t, end_t) < - 12
        THEN datediff(part, start_t, TIME '23:59:59') + datediff(part, TIME '00:00:00', end_t)
    ELSE 
        datediff(part, start_t, end_t)
END;

CREATE OR REPLACE TABLE delays AS
    WITH 
        -- Summarizes the arrival times for each trip and stop
        arrivals AS (
            SELECT 
                global_trip_id, 
                route_id, 
                trip_id, 
                stop_id, 
                current_stop_sequence, 
                max(timestamp) AS actual_arrival,
                CAST(strftime(max(timestamp), '%H:%M:%S') AS TIME) AS actual_arrival_time
            FROM positions 
            GROUP BY global_trip_id, route_id, trip_id, stop_id, current_stop_sequence
        )
    SELECT
        a.global_trip_id,
        a.route_id,
        a.trip_id,
        a.current_stop_sequence,

        -- Edge in graph: from and to stop_ids
        COALESCE(LAG(a.stop_id) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), a.stop_id) AS from_stop_id,
        a.stop_id AS to_stop_id,

        -- What actually happened?
        a.actual_arrival,
        COALESCE(LAG(a.actual_arrival) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), a.actual_arrival) AS actual_departure,
        a.actual_arrival_time,
        datediff('second', actual_departure, actual_arrival)::INT AS actual_travel_time,

        -- What is expected?
        CAST(
            lpad((left(st.arrival_time, 2)::INT % 24)::VARCHAR, 2, '0') || substring(st.arrival_time, 3) AS TIME
        ) AS target_arrival,
        COALESCE(LAG(target_arrival) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), target_arrival) AS target_departure,
        CAST(timediff('second', target_departure, target_arrival) AS INT) AS target_travel_time,

        -- Delays
        CAST(timediff('second', target_arrival, a.actual_arrival_time) AS INT) AS delay,
        COALESCE(SUM(delay) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::INT AS cummulative_delay
    FROM arrivals a
    JOIN stop_times st
        ON a.trip_id = st.trip_id AND a.current_stop_sequence = st.stop_sequence