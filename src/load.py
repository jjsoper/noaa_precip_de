import json
import uuid
from datetime import datetime, timezone
from typing import Any

from google.cloud.bigquery.job import LoadJob

from src import settings
from src.logging.custom_logger import get_logger
from src.managers.bigquery_manager import BigQueryManager

logger = get_logger()


def load_bronze_precip_raw_noaa_station_observations(
    bigquery_manager: BigQueryManager,
    table: str,
    raw_records: list[dict[str, Any]],
    job_id: str,
    event_id: str,
) -> LoadJob:
    """
    Load raw NOAA station observations into the bronze BigQuery table.

    This function takes a raw JSON response from the NOAA API and loads it into
    BigQuery with the schema: _record_id, raw_response_json, _job_id, _event_id,
    _record_created_at, _record_updated_at.

    Args:
        bigquery_manager: An instance of BigQueryManager for performing operations
        table: The name of the BigQuery table to load data into (not a fully qualified table path)
        raw_records: List of raw records from the NOAA API containing observations
        job_id: ID of the data ingestion job
        event_id: ID of the event that triggered the job.

    Returns:
        LoadJob: The BigQuery load job

    Raises:
        ValueError: If raw_response is empty or invalid
        google.cloud.exceptions.GoogleCloudError: If BigQuery operation fails
    """
    if not raw_records:
        raise ValueError("raw_response cannot be empty")

    records = [
        {
            "_record_id": str(uuid.uuid4()),
            "raw_response_json": json.dumps(record),
            "_job_id": job_id,
            "_event_id": event_id,
            # INSERT statements won't honor default timestamps, so we need to set these here
            "_record_created_at": datetime.now(timezone.utc).isoformat(),
            "_record_updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for record in raw_records
    ]

    return bigquery_manager.load_from_json(
        records=records,
        table=table,
        write_disposition="WRITE_APPEND",
    )
