import argparse
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import gdown
from tqdm import tqdm

DATASET_URL = "https://drive.google.com/file/d/1X3631ort-3bz9H0s7bQikK9zk2NE7tNV/view?usp=sharing"

logger = logging.getLogger(__name__)


def extract_zip_to_dir(zip_file_handle, target_dir: str) -> list[Path]:
    os.makedirs(target_dir, exist_ok=True)
    with zipfile.ZipFile(zip_file_handle) as zip_ref:
        members = zip_ref.infolist()
        total = len(members)
        logger.info(f"Extracting zipfile to '{target_dir}' ({total} files)...")
        for member in tqdm(members, desc="Extracting", unit="file"):
            zip_ref.extract(member, target_dir)
    return [Path(target_dir) / m.filename for m in members]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download and extract dataset if target directory is empty."
    )
    parser.add_argument(
        "--output-path",
        "-o",
        type=str,
        required=True,
        help="Path to the target directory to extract files into.",
    )
    parser.add_argument(
        "--gdrive-url",
        "-u",
        type=str,
        default=DATASET_URL,
        help="Google Drive URL of the zip file to download.",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    output_path = Path(args.output_path)
    output_path.mkdir(exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".zip") as tmpfile:
        temp_zip_path = tmpfile.name
        gdown.download(args.gdrive_url, output=temp_zip_path, fuzzy=True, quiet=False)
        logger.info(f"Downloaded zip file to temporary location: {temp_zip_path}")

        try:
            extracted_files = extract_zip_to_dir(temp_zip_path, str(output_path))
        except Exception as e:
            logger.error(f"Failed to extract zipfile: {e}")
            exit(1)

    logger.info(
        f"Extracted {len(extracted_files)} files: {[str(f.absolute()) for f in extracted_files]}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
