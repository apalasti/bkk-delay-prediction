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
        # conn.execute(f"CREATE TABLE routes AS SELECT * FROM read_parquet('{data_dir / "static/routes.parquet"}')")
        # conn.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{data_dir / "static/trips.parquet"}')")
        # conn.execute(f"CREATE TABLE stops AS SELECT * FROM read_parquet('{data_dir / "static/stops.parquet"}')")
        
        num_rows = conn.table("positions").count("1").fetchone()[0]
        logger.info(f"Loaded positions table with {num_rows:,} rows")

        # Perform processing
        steps = [
            remove_duplicates_and_invalid,
            create_global_trip_id,
            clean_stops,
            extend_with_arrivals,
            extend_with_date,
        ]
        for i, fn in enumerate(steps, 1):
            logger.info(f"Executing step with name: '{fn.__name__}' ({i} / {len(steps)})")

            start_time = time.perf_counter()
            fn(conn)
            elapsed_time = time.perf_counter() - start_time

            num_rows = conn.table("positions").count("1").fetchone()[0]
            logger.info(f"Step '{fn.__name__}' completed in {elapsed_time:.4f} seconds with {num_rows:,} rows in table")
        print("New format of positions:")
        print(conn.sql(f"DESCRIBE positions"))

        # Save data to disk
        output_dir.mkdir(parents=True, exist_ok=True)
        conn.execute(f"""COPY positions TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (date), APPEND)""")
        logger.info(f"Saved positions to: {output_dir.absolute()}")


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


def clean_stops(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    SELECT p.* EXCLUDE (current_stop_sequence, stop_id),
        MIN(current_stop_sequence) OVER (
            PARTITION BY global_trip_id
            ORDER BY timestamp
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS current_stop_sequence,
        st.stop_id AS stop_id
    FROM positions p
    JOIN stop_times st ON 
        p.trip_id = st.trip_id AND current_stop_sequence = st.stop_sequence""")


def extend_with_arrivals(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    WITH 
        a AS (
            SELECT global_trip_id, stop_id, max(timestamp) AS timestamp
            FROM positions GROUP BY global_trip_id, stop_id
        )
    SELECT p.*, st.arrival_time AS target_arrival, strftime(a.timestamp, '%H:%M:%S') AS actual_arrival
    FROM positions p
    JOIN stop_times st
        ON p.trip_id = st.trip_id AND p.stop_id = st.stop_id
    JOIN a
        ON p.global_trip_id = a.global_trip_id AND p.stop_id = a.stop_id
    """)


def extend_with_date(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE OR REPLACE TABLE positions AS
    SELECT *,
        DATE(timestamp) AS date
    FROM positions
    """)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
