import json
import logging
import uuid

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

    logger.info(f"Successfully extracted observations for station: {station_id}")

    return blob_name


if __name__ == "__main__":
    test_args = {
        "station_id": "KBOS",
        "trace_id": str(uuid.uuid4()),
        "start": "2026-04-23T17:40:00+00:00",
        "end": "2026-04-24T17:40:00+00:00",
    }
    extract_noaa_observations(**test_args)
