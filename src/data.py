import pathlib
import logging

import duckdb

logger = logging.getLogger(__name__)


def load_data(data_dir, database: str):
    data_dir = pathlib.Path(data_dir)
    conn = duckdb.connect(database)

    # Load tables
    for table_name in ["positions", "stop_times", "delays", "stops"]:
        fpath = data_dir / f"{table_name}.parquet"
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{fpath}')"
        )
    return conn


def run_sql_file(conn: duckdb.DuckDBPyConnection, file_path: str):
    with open(file_path) as f:
        conn.execute(f.read())


def create_table_from_files(conn: duckdb.DuckDBPyConnection, table_name: str, data_dir):
    data_dir = pathlib.Path(data_dir)
    pattern = f"**/*{table_name}*.parquet"

    num_files = 0
    for _ in data_dir.glob(pattern):
        num_files += 1

    pattern = data_dir.absolute() / pattern
    conn.execute(
        f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet($pattern)",
        parameters={"pattern": str(pattern)},
    )
    logger.info(f"Created table '{table_name}' with {num_files} parquet files matching pattern: {pattern}")
