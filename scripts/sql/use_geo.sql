-- Use geo columns for coordinate data 
install spatial;
load spatial;

CREATE OR REPLACE TABLE positions AS
    SELECT *, ST_Point(latitude, longitude) AS pos FROM positions;

CREATE OR REPLACE TABLE stops AS
    SELECT *,
        ST_Point(stop_lat, stop_lon) AS stop_pos 
    FROM stops;
