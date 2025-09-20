import zipfile
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def fetch_static_gtfs_data(url: str, timeout: int = 30) -> dict[str, pd.DataFrame]:
    """
    Download GTFS static data zip file, extract CSV files, and return as DataFrames.

    Args:
        url (str): URL of the GTFS zip file to download
        timeout (int): Timeout for the HTTP request in seconds

    Returns:
        dict[str, pd.DataFrame]: Dictionary mapping file names to DataFrames
    """
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    try:
        logger.info(f"Downloading GTFS data from {url}")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file_path = Path(temp_dir) / f"gtfs_{timestamp}.zip"
            with open(zip_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
            logger.info(f"Downloaded zip file to {zip_file_path}")

            dataframes = {}
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                # Get list of CSV files in the zip
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.txt')]
                logger.info(f"Found {len(csv_files)} text files in zip: {csv_files}")

                for csv_file in csv_files:
                    file_name = Path(csv_file).stem
                    try:
                        with zip_ref.open(csv_file) as file:
                            df = pd.read_csv(file)
                            object_cols = df.select_dtypes(include=["object"]).columns
                            df[object_cols] = df[object_cols].astype("string")
                            dataframes[file_name] = df
                    except Exception as e:
                        logger.error(f"Error processing {csv_file}: {e}")
                        continue
            return dataframes
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading GTFS data: {e}")
        return {}
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting zip file: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dataframes = fetch_static_gtfs_data(
        url="https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
    )
    print(dataframes)
