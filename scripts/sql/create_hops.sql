CREATE OR REPLACE TABLE stop_times AS
    SELECT * EXCLUDE (departure_time, arrival_time),
        CAST(lpad((left(departure_time, 2)::INT % 24)::VARCHAR, 2, '0') || substring(departure_time, 3) AS TIME) AS departure_time,
        CAST(lpad((left(arrival_time, 2)::INT % 24)::VARCHAR, 2, '0') || substring(arrival_time, 3) AS TIME) AS arrival_time,
    FROM stop_times;

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
                stop_id, 
                current_stop_sequence, 
                max(timestamp) AS timestamp,
                argmax(pos, timestamp) AS pos
            FROM positions 
            WHERE stop_id IS NOT NULL
            GROUP BY global_trip_id, route_id, trip_id, vehicle_id, stop_id, current_stop_sequence
        )
    SELECT
        a.global_trip_id,
        a.route_id,
        a.trip_id,
        a.current_stop_sequence,

        COALESCE(LAG(a.stop_id) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), a.stop_id) AS from_stop_id,
        a.stop_id AS to_stop_id,

        COALESCE(LAG(a.timestamp) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), a.timestamp) AS departure_timestamp,
        a.timestamp AS arrival_timestamp,

        datediff('second', departure_timestamp, arrival_timestamp)::INT AS duration,

        COALESCE(LAG(st.departure_time) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), st.departure_time) AS expected_departure,
        st.arrival_time AS expected_arrival,

        timediff('second', expected_departure, expected_arrival)::INT AS expected_duration,

        -- delay in seconds
        timediff('second', expected_arrival, strftime(arrival_timestamp, '%H:%M:%S')::TIME)::INT AS delay,

        COALESCE(LAG(s.stop_pos) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), s.stop_pos) AS start_pos,
        s.stop_pos AS end_pos,

        -- distance between stops according to shape info
        st.shape_dist_traveled - COALESCE(LAG(st.shape_dist_traveled) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), st.shape_dist_traveled) AS distance,

        -- distance from target when arrival is registered
        ST_Distance(a.pos, end_pos) * 111111 AS distance_from_target,

    FROM arrivals a
    JOIN stops s ON a.stop_id = s.stop_id
    JOIN stop_times st
        ON a.trip_id = st.trip_id AND a.current_stop_sequence = st.stop_sequence
