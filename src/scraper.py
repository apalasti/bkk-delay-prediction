import time
import os
import pandas as pd
import logging
import threading
import io
from datetime import datetime
from pathlib import Path
from itertools import groupby

import schedule
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from .fetch.vehicle_positions import fetch_vehicle_positions
from .fetch.alerts import fetch_alerts

load_dotenv()
logger = logging.getLogger(__name__)

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
POSITIONS_CONTAINER = os.getenv("POSITIONS_CONTAINER", "positions")
ALERTS_CONTAINER = os.getenv("ALERTS_CONTAINER", "alerts")


def upload_df_to_azure(df: pd.DataFrame, connection: str, container_name: str, filename: str):
    blob_service_client = BlobServiceClient.from_connection_string(connection)
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
        logger.info(f"Created container: '{container_name}'")

    blob_client = container_client.get_blob_client(blob=filename)
    data = df.to_parquet(index=False)
    blob_client.upload_blob(data, overwrite=True)
    logger.info(f"Uploaded to '{container_name}': '{filename}' ({len(df)} rows)")


def merge_parquets(connection: str, container_name: str):
    logger.info(f"Starting merge for container: '{container_name}'")
    blob_service_client = BlobServiceClient.from_connection_string(connection)
    container_client = blob_service_client.get_container_client(container_name)

    parquet_files = sorted(
        name
        for name in container_client.list_blob_names()
        if name.endswith(".parquet")
        and len(name.rstrip(".parquet")) >= 6
        and name.rstrip(".parquet")[-6:].isdigit()
    )
    for key, group in groupby(parquet_files, lambda name: name.rstrip(".parquet")[:-4]):
        group = list(group)
        if len(group) < 2:
            logger.info(f"Skipping group '{key}': less than 2 files to merge.")
            continue

        try:
            grouped_dfs = []
            for file_name in group:
                blob_client = container_client.get_blob_client(file_name)
                with io.BytesIO() as b:
                    blob_client.download_blob().readinto(b)
                    grouped_dfs.append(pd.read_parquet(b))

            merged_file = f"{key}.parquet"
            merged_df = pd.concat(grouped_dfs)
            upload_df_to_azure(merged_df, connection, container_name, merged_file)

            for file_name in group:
                blob_client = container_client.get_blob_client(file_name)
                blob_client.delete_blob()
            logger.info(f"Successfully merged {len(group)} files into: '{merged_file}'")
        except Exception:
            logger.error(f"Error during merging and uploading parquet files for key '{key}':", exc_info=True)


def save_positions(container_name: str = None):
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H:%M:%S")

    logger.info(f"Fetching vehicle positions at {date_str} {time_str} ...")
    df = fetch_vehicle_positions()
    if df.empty:
        logger.info("No vehicle positions fetched.")
        return

    filename = f"{date_str}/vehicle_positions_{time_str.replace(':', '')}.parquet"
    if CONNECTION_STRING is not None and container_name is not None:
        try:
            upload_df_to_azure(df, CONNECTION_STRING, container_name, filename)
        except Exception as e:
            logger.error(f"Failed to upload {filename} to Azure Blob Storage: {e}")
    else:
        file_path = Path(__file__).parent.parent / "data" / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved vehicle positions to {file_path} ({len(df)} rows)")


def save_alerts(container_name: str = None):
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H:%M:%S")

    logger.info(f"Fetching alerts at {date_str} {time_str} ...")
    df = fetch_alerts()
    if df.empty:
        logger.info("No alerts fetched.")
        return

    filename = f"{date_str}/alerts_{time_str.replace(':', '')}.parquet"
    if CONNECTION_STRING is not None and container_name is not None:
        try:
            upload_df_to_azure(df, CONNECTION_STRING, container_name, filename)
        except Exception as e:
            logger.error(f"Failed to upload {filename} to Azure Blob Storage: {e}")
    else:
        file_path = Path(__file__).parent.parent / "data" / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved alerts to {file_path} ({len(df)} rows)")


def main():
    def run_threaded(job_func, *args, **kwargs):
        job_thread = threading.Thread(target=job_func, args=args, kwargs=kwargs)
        job_thread.start()
        return job_thread

    # Run them first
    threads = [
        run_threaded(save_positions, POSITIONS_CONTAINER),
        run_threaded(save_alerts, ALERTS_CONTAINER)
    ]
    for thread in threads:
        thread.join()

    # Schedule hourly compaction for vehicle positions data
    if CONNECTION_STRING is not None:
        schedule.every().hour.do(run_threaded, merge_parquets, CONNECTION_STRING, POSITIONS_CONTAINER)

    schedule.every(15).seconds.do(run_threaded, save_positions, POSITIONS_CONTAINER)
    schedule.every().day.do(run_threaded, save_alerts, ALERTS_CONTAINER)

    logger.info("Scheduler loop starting ...")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logging.getLogger("azure.core").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
