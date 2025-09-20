import os
import logging
from typing import Literal

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

FEED_TYPES = Literal["vehicle_pos", "trip_updates", "alerts"]

load_dotenv()
logger = logging.getLogger(__name__)


def get_url(feed_type: FEED_TYPES):
    BASE_URL = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full"
    urls = {
        "vehicle_pos": f"{BASE_URL}/VehiclePositions.pb",
        "trip_updates": f"{BASE_URL}/TripUpdates.pb",
        "alerts": f"{BASE_URL}/Alerts.pb",
    }
    assert (
        feed_type in urls.keys()
    ), f"Invalid feed_type '{feed_type}'. Valid options are: {list(urls.keys())}"
    return urls[feed_type]


def fetch_trainsit_feed(feed_type: FEED_TYPES, api_key=None, timeout=10):
    """
    Fetch and parse a GTFS-realtime feed from the specified URL.

    Args:
        feed_type (FEED_TYPES): Type of GTFS-realtime feed to fetch (e.g., "vehicle_pos").
        api_key (str, optional): API key for authentication. If None, uses BKK_API_KEY from environment.
        timeout (int, optional): Timeout for the HTTP request in seconds.

    Returns:
        gtfs_realtime_pb2.FeedMessage: Parsed GTFS-realtime feed message.
    """
    if api_key is None:
        api_key = os.getenv("BKK_API_KEY")
    if not api_key:
        raise ValueError(
            "API key must be provided via parameter or BKK_API_KEY environment variable."
        )

    url = get_url(feed_type)
    try:
        response = requests.get(url, params={"key": api_key}, timeout=timeout)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Error fetching data: {req_err}")
    except Exception as e:
        logger.error(f"Error parsing data: {e}")
