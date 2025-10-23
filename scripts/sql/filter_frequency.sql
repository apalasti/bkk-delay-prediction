CREATE OR REPLACE TABLE positions AS
    SELECT 
        first(COLUMNS(p.*))
    FROM positions p
    GROUP BY global_trip_id, datetrunc('minutes', p.timestamp);
