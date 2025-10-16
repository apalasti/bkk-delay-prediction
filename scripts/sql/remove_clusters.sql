CREATE TEMP TABLE clusters AS
    SELECT
        p1.global_trip_id,
        p1.current_stop_sequence,
        p1.timestamp,
        count(1) AS count
    FROM positions p1
    JOIN positions p2 ON p1.global_trip_id = p2.global_trip_id 
        AND p1.current_stop_sequence = p2.current_stop_sequence
        AND p1.timestamp != p2.timestamp
        AND ST_Distance(p1.pos, p2.pos) * 111111 < 40 -- What is called "in proximity"
    GROUP BY p1.global_trip_id, p1.current_stop_sequence, p1.timestamp
    HAVING count > 15; -- How many points are in proximity to this

CREATE OR REPLACE TABLE positions AS
    SELECT 
        p.* EXCLUDE (current_stop_sequence, stop_id),
        IF(
            c.count IS NOT NULL,
            NULL,
            p.current_stop_sequence
        ) AS current_stop_sequence,
        IF(
            c.count IS NOT NULL,
            NULL,
            p.stop_id
        ) AS stop_id,
    FROM positions p
    LEFT JOIN clusters c ON 1=1
        AND p.global_trip_id = c.global_trip_id
        AND p.current_stop_sequence = c.current_stop_sequence
        AND p.timestamp = c.timestamp
    ORDER BY p.global_trip_id, p.timestamp;