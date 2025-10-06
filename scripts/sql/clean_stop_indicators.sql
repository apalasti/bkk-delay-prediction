CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (
            id, 
            vehicle_label, 
            vehicle_license_plate,
            current_status, 
            time_diff, 
            current_stop_sequence, 
            stop_id
        ),
        MIN(current_stop_sequence) OVER (
            PARTITION BY p.global_trip_id
            ORDER BY timestamp
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS current_stop_sequence,
        st.stop_id AS stop_id
    FROM positions p
    LEFT JOIN stop_times st ON
        p.trip_id = st.trip_id AND current_stop_sequence = st.stop_sequence;