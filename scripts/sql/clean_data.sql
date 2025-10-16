-- Remove duplicates and invalid rows
CREATE OR REPLACE TABLE positions AS
    SELECT DISTINCT * FROM positions WHERE trip_id IS NOT NULL;


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
    JOIN trips t ON t.trip_id = st.trip_id;

CREATE OR REPLACE TABLE positions AS
    SELECT 
        p.* EXCLUDE(
            id, 
            vehicle_label, 
            vehicle_license_plate,
            current_status
        ) 
    FROM positions p
    JOIN trips t ON p.trip_id = t.trip_id
    ORDER BY p.trip_id, p.vehicle_id, p.current_stop_sequence, p.timestamp;


-- Trim position frequency to be only 1 position per minute
CREATE OR REPLACE TABLE positions AS
    SELECT first(COLUMNS(p.*))
    FROM positions p
    GROUP BY 
        trip_id, 
        vehicle_id, 
        current_stop_sequence, 
        date_trunc('seconds', p.timestamp);
