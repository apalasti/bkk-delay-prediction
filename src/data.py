import os
import pathlib
import logging

import duckdb

logger = logging.getLogger(__name__)


def load_data(data_dir, database: str):
    conn = duckdb.connect(database)
    for table_name in ["positions", "stop_times", "hops", "trips", "stops"]:
        create_table_from_files(conn, data_dir, table_name)

    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.execute("""
CREATE OR REPLACE TABLE positions AS
    SELECT * EXCLUDE (pos),
        ST_GeomFromWKB(pos)::POINT_2D AS pos,
    FROM positions;

CREATE OR REPLACE TABLE stops AS
    SELECT * EXCLUDE (stop_pos),
        ST_GeomFromWKB(stop_pos)::POINT_2D AS stop_pos,
    FROM stops;
    
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
END;""")

    return conn


def run_sql_file(conn: duckdb.DuckDBPyConnection, file_path: os.PathLike):
    with open(file_path) as f:
        conn.execute(f.read())


def create_table_from_files(conn: duckdb.DuckDBPyConnection, data_dir, table_name: str):
    data_dir = pathlib.Path(data_dir)
    pattern = f"**/*{table_name}*.parquet"

    num_files = 0
    for _ in data_dir.glob(pattern):
        num_files += 1

    if num_files == 0:
        raise FileNotFoundError(
            f"No parquet files found for table '{table_name}' in directory {data_dir} with pattern {pattern}"
        )

    pattern = data_dir.absolute() / pattern
    conn.execute(
        f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet($pattern)",
        parameters={"pattern": str(pattern)},
    )
    logger.info(f"Created table '{table_name}' with {num_files} parquet files matching pattern: {pattern}")
