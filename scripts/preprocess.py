import pathlib
import argparse
import logging
import time

import duckdb

from src.data import run_sql_file, create_table_from_files

SQL_SCRIPTS_DIR = pathlib.Path(__file__).parent / "sql"
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Process transit data and generate outputs")
    parser.add_argument(
        "--inputs-dir", type=str, required=True,
        help="Path to directory containing parquet files to be processed"
    )
    parser.add_argument(
        "--outputs-dir", "-o", type=str, required=True,
        help="Path to directory where processed outputs should be saved"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    inputs_dir = pathlib.Path(args.inputs_dir)
    output_dir = pathlib.Path(args.outputs_dir)

    with duckdb.connect(":memory:") as conn: 
        logger.info(f"Loading parquet files from: {inputs_dir}")
        create_table_from_files(conn, inputs_dir, "positions")
        create_table_from_files(conn, inputs_dir, "stop_times")
        create_table_from_files(conn, inputs_dir, "stops")
        
        num_rows = conn.table("positions").count("1").fetchone()[0]
        logger.info(f"Loaded positions table with {num_rows:,} rows")

        # Perform processing
        steps = [
            "remove_duplicates",
            "attach_global_trip_id",
            "clean_stop_indicators",
            "create_delays",
        ]
        for i, step_name in enumerate(steps, 1):
            logger.info(f"Executing step with name: '{step_name}'".ljust(70, " ") + f"({i} / {len(steps)})")
            start_time = time.perf_counter()
            run_sql_file(conn, SQL_SCRIPTS_DIR / f"{step_name}.sql")
            elapsed_time = time.perf_counter() - start_time
            logger.info(f"Step '{step_name}' completed in {elapsed_time:.4f} seconds")

        # Save data to disk
        output_dir.mkdir(parents=True, exist_ok=True)
        for table in conn.execute("SHOW TABLES").fetchall():
            table_name = table[0]
            file_path = output_dir / f"{table_name}.parquet"
            conn.execute(f"COPY {table_name} TO '{file_path}' (FORMAT PARQUET)")
            logger.info(f"Saved {table_name} to: {file_path.absolute()}")

            num_rows = conn.table(table_name).count("1").fetchone()[0]
            print(f"SCHEMA of '{table_name}' ({num_rows:,}):")
            print(conn.sql(f"SUMMARIZE {table_name}"))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
