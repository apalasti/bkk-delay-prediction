import argparse
import logging
from pathlib import Path

from src.fetch.static import fetch_static_gtfs_data


def save_dataframes_to_parquet(dataframes: dict, output_dir: Path) -> None:
    """
    Save DataFrames to parquet files in the specified directory.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        output_dir: Directory to save parquet files
    """
    logger = logging.getLogger(__name__)
    
    if not dataframes:
        logger.warning("No dataframes to save")
        return
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving {len(dataframes)} files to {output_dir}")
    
    saved_files = []
    for filename, df in dataframes.items():
        try:
            parquet_path = output_dir / f"{filename}.parquet"
            df.to_parquet(parquet_path, index=False)
            saved_files.append(parquet_path)
            logger.info(f"Saved {filename} -> {parquet_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to save {filename}: {e}")
    
    logger.info(f"Successfully saved {len(saved_files)} parquet files")
    

def parse_args():
    parser = argparse.ArgumentParser( description="Download GTFS static data and save as parquet files")
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default="data/raw",
        help="Output directory for parquet files"
    )
    parser.add_argument(
        "-u", "--url",
        type=str,
        default="https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip",
        help="URL of the GTFS zip file (default: Budapest GTFS)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    dataframes = fetch_static_gtfs_data(args.url)
    for name, df in dataframes.items():
        parquet_path = output_dir / f"{name}.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"Saved {name} -> {parquet_path} ({len(df)} rows)")
        print(df.info(verbose=True))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    main()
