CREATE OR REPLACE TABLE positions AS
    WITH
        stops_visited AS (
            SELECT
                global_trip_id,
                trip_id,
                count(DISTINCT stop_id) AS stop_count
            FROM positions
            GROUP BY global_trip_id, trip_id
        ),
        stop_counts AS (
            SELECT
                trip_id,
                count(DISTINCT stop_id) AS stop_count
            FROM stop_times
            GROUP BY trip_id
        ),
        full_trips AS (
            SELECT 
                sv.global_trip_id
            FROM stops_visited sv
            JOIN stop_counts sc ON
                sc.trip_id = sv.trip_id AND sc.stop_count = sv.stop_count
        )
    SELECT p.* FROM positions p
    WHERE p.global_trip_id IN (SELECT global_trip_id FROM full_trips);
