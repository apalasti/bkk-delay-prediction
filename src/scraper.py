from ast import arg
import time
import os
import pandas as pd
import logging
import threading
from datetime import datetime
from pathlib import Path

import schedule
from azure.storage.blob import BlobServiceClient

from .fetch.vehicle_positions import fetch_vehicle_positions
from .fetch.alerts import fetch_alerts

logger = logging.getLogger(__name__)
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")


def upload_df_to_azure(df: pd.DataFrame, connection: str, container_name: str, filename: str):
    blob_service_client = BlobServiceClient.from_connection_string(connection)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob=filename)

    data = df.to_parquet(index=False)
    blob_client.upload_blob(data, overwrite=True)
    logger.info(f"Uploaded to '{container_name}': '{filename}' ({len(df)} rows)")


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
        run_threaded(save_positions, "positions"),
        run_threaded(save_alerts, "alerts")
    ]
    for thread in threads:
        thread.join()

    schedule.every(15).seconds.do(run_threaded, save_positions, "positions")
    schedule.every(12).hours.do(run_threaded, save_alerts, "alerts")

    logger.info("Scheduler loop starting ...")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
