import json
import logging
import uuid

import pendulum
from google.cloud import storage

from src.bronze_noaa_station import settings
from src.managers.noaa_api_manager import NOAAWeatherManager

logger = logging.getLogger(__name__)


storage_client = storage.Client()
bucket = storage_client.bucket(settings.BUCKET)
noaa_manager = NOAAWeatherManager()


def extract_noaa_observations(
    station_id: str, start: str, end: str, trace_id: str
) -> str:
    """Extract NOAA observations and write to GCS

    Args:
        station_id (str): ID of the station to extract observations for
        start (str): Start date for observations in ISO format (e.g. "2026-01-01T00:00:00+00:00")
        end (str): End date for observations in ISO format (e.g. "2026-01-31T23:59:59+00:00")
        trace_id (str): Trace ID for observability

    Returns:
        str: GCS bucket file path
    """

    job_id = str(uuid.uuid4())

    response = noaa_manager.fetch_observations(
        station_id=station_id,
        start=start,
        end=end,
    )

    station_observations_with_meta = {
        "trace_id": trace_id,
        "observations": response.json(),
    }

    blob_name = f"{job_id}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(station_observations_with_meta))

    logger.info(
        f"Successfully extracted observations for station: {station_id}. Loaded to GCS at: {blob_name}"
    )

    return blob_name


if __name__ == "__main__":
    from src.logging.custom_logger import get_logger

    logger = get_logger()
    start = pendulum.now().subtract(days=1).to_iso8601_string()
    end = pendulum.now().to_iso8601_string()
    blob_name = extract_noaa_observations(
        station_id="KBOS",
        start=start,
        end=end,
        trace_id=str(uuid.uuid4()),
    )
