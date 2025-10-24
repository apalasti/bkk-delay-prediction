-- NOTE: We define the arrival as the maximum timestep where in each stop, this
-- has a negative effect: what if the bus stays in the same place for a long time
CREATE OR REPLACE TABLE hops AS
    WITH 
        -- Summarizes the arrival times for each trip and stop
        arrivals AS (
            SELECT 
                global_trip_id, 
                route_id, 
                trip_id, 
                vehicle_id,
                current_stop_sequence, 
                max(timestamp) AS timestamp,
                argmax(pos, timestamp) AS pos
            FROM positions 
            WHERE current_stop_sequence IS NOT NULL
            GROUP BY global_trip_id, route_id, trip_id, vehicle_id, current_stop_sequence
        )
    SELECT
        a.global_trip_id,
        a.trip_id,
        a.current_stop_sequence,

        COALESCE(LAG(st.stop_id) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), st.stop_id) AS from_stop_id,
        st.stop_id AS to_stop_id,

        COALESCE(LAG(a.timestamp) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), a.timestamp) AS actual_departure,
        a.timestamp AS actual_arrival,

        datediff('second', actual_departure, actual_arrival)::INT AS actual_duration,

        -- distance from target when arrival is registered
        ST_Distance(a.pos, s.stop_pos) * 111111 AS distance_from_target,

    FROM arrivals a
    JOIN stop_times st ON a.trip_id = st.trip_id 
        AND a.current_stop_sequence = st.stop_sequence
    JOIN stops s ON st.stop_id = s.stop_id;
