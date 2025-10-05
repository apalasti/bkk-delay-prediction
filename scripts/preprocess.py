import pathlib
import argparse
import logging
import time

import duckdb

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Process transit data and generate outputs")
    parser.add_argument(
        "--positions-dir", type=str, required=True,
        help="Path to directory containing parquet files to be processed"
    )
    parser.add_argument(
        "--static-dir", type=str, required=True,
        help="Path to directory containing static parquet files (routes, trips, stops, stop_times)"
    )
    parser.add_argument(
        "--output", "-o", type=str, required=True,
        help="Path to directory where processed outputs should be saved"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    positions_dir = pathlib.Path(args.positions_dir)
    static_dir = pathlib.Path(args.static_dir)
    output_dir = pathlib.Path(args.output)

    with duckdb.connect(":memory:") as conn: 
        logger.info(f"Loading parquet files from: {positions_dir}, {static_dir}")
        conn.execute(f"CREATE TABLE positions AS SELECT * FROM read_parquet('{positions_dir / "**/*.parquet"}')")
        conn.execute(f"CREATE TABLE stop_times AS SELECT * FROM read_parquet('{static_dir / "stop_times.parquet"}')")
        # conn.execute(f"CREATE TABLE routes AS SELECT * FROM read_parquet('{static_dir / "routes.parquet"}')")
        # conn.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{data_dir / "static/trips.parquet"}')")
        conn.execute(f"CREATE TABLE stops AS SELECT * FROM read_parquet('{static_dir / "stops.parquet"}')")
        
        num_rows = conn.table("positions").count("1").fetchone()[0]
        logger.info(f"Loaded positions table with {num_rows:,} rows")

        # Perform processing
        steps = [
            remove_duplicates_and_invalid,
            create_global_trip_id,
            clean_positions,
            remove_partial_trips,
            create_delays,
        ]
        for i, fn in enumerate(steps, 1):
            logger.info(f"Executing step with name: '{fn.__name__}'".ljust(70, " ") + f"({i} / {len(steps)})")
            start_time = time.perf_counter()
            fn(conn)
            elapsed_time = time.perf_counter() - start_time
            logger.info(f"Step '{fn.__name__}' completed in {elapsed_time:.4f} seconds")

        # Save data to disk
        output_dir.mkdir(parents=True, exist_ok=True)
        for table in conn.execute("SHOW TABLES").fetchall():
            table_name = table[0]
            file_path = output_dir / f"{table_name}.parquet"
            conn.execute(f"COPY {table_name} TO '{file_path}' (FORMAT PARQUET)")
            logger.info(f"Saved {table_name} to: {file_path.absolute()}")

            num_rows = conn.table(table_name).count("1").fetchone()[0]
            print(f"SCHEMA of '{table_name}' ({num_rows:,}):")
            print(conn.sql(f"DESCRIBE {table_name}"))


def remove_duplicates_and_invalid(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
        SELECT DISTINCT * FROM positions WHERE trip_id IS NOT NULL
    """)


def create_global_trip_id(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    SELECT *,
        COALESCE(
            timestamp - LAG(timestamp) OVER (
                PARTITION BY route_id, trip_id, vehicle_id 
                ORDER BY timestamp
            ),
            INTERVAL '1 second'
        ) AS time_diff
    FROM positions;""")
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    SELECT *,
        SUM(
            CASE WHEN time_diff > INTERVAL '5 minutes' THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY route_id, trip_id, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS local_trip_id
    FROM positions;""")
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    SELECT * EXCLUDE (local_trip_id),
        hash(route_id, trip_id, vehicle_id, local_trip_id) AS global_trip_id
    FROM positions;""")


def clean_positions(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
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
        p.trip_id = st.trip_id AND current_stop_sequence = st.stop_sequence""")


def remove_partial_trips(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    WITH
        stops_visited AS (
            SELECT
                global_trip_id,
                trip_id,
                count(DISTINCT stop_id) AS stops
            FROM positions
            GROUP BY global_trip_id, trip_id
        ),
        all_stops AS (
            SELECT 
                trip_id,
                count(DISTINCT stop_id) AS stops
            FROM stop_times
            GROUP BY trip_id
        )
    SELECT p.* FROM positions p
    JOIN stops_visited sv ON p.global_trip_id = sv.global_trip_id
    JOIN all_stops ON p.trip_id = all_stops.trip_id
    WHERE sv.stops = all_stops.stops""")


def create_delays(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
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
        arrivals AS (
            SELECT 
                global_trip_id, 
                route_id, 
                trip_id, 
                stop_id, 
                current_stop_sequence, 
                CAST(strftime(max(timestamp), '%H:%M:%S') AS TIME) AS actual_arrival
            FROM positions 
            GROUP BY global_trip_id, route_id, trip_id, stop_id, current_stop_sequence
        )
    SELECT
        a.*,
        CAST(
            lpad((left(st.arrival_time, 2)::INT % 24)::VARCHAR, 2, '0') || substring(st.arrival_time, 3) AS TIME
        ) AS target_arrival,
        COALESCE(LAG(target_arrival) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
        ), target_arrival) AS target_start,
        CAST(timediff('second', target_start, target_arrival) AS INT) AS travel_time,
        CAST(timediff('second', target_arrival, actual_arrival) AS INT) AS delay,
        COALESCE(SUM(delay) OVER (
            PARTITION BY a.global_trip_id
            ORDER BY a.current_stop_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS cummulative_delay
    FROM arrivals a
    LEFT JOIN stop_times st
        ON a.trip_id = st.trip_id AND a.current_stop_sequence = st.stop_sequence
    """)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
