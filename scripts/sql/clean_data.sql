-- Remove duplicates and invalid rows
CREATE OR REPLACE TABLE positions AS
    SELECT DISTINCT * FROM positions p WHERE p.trip_id IS NOT NULL;


-- Localize timestamps
CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (timestamp),
        timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Budapest' AS timestamp
    FROM positions p;


-- Filter to the top 100 routes
CREATE OR REPLACE TABLE trips AS
    WITH 
        routes_of_interest AS (
            SELECT 
                route_id,
                -- trip_headsign, 
                -- direction_id, 
                count(1) AS count
            FROM trips
            GROUP BY route_id --, trip_headsign, direction_id 
            ORDER BY count DESC
            LIMIT 100
        )
    SELECT t.* FROM trips t
    JOIN routes_of_interest roi ON t.route_id = roi.route_id;

CREATE OR REPLACE TABLE stop_times AS
    SELECT st.*
    FROM stop_times st
    WHERE st.trip_id IN (SELECT trip_id FROM trips);

CREATE OR REPLACE TABLE positions AS
    SELECT 
        p.* EXCLUDE(
            id, 
            vehicle_label, 
            vehicle_license_plate,
            current_status
        ) 
    FROM positions p
    WHERE p.trip_id IN (SELECT trip_id FROM trips);

-- Clean stop_times
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
