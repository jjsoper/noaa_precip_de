import json
import logging
import uuid
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud.bigquery.job import LoadJob

from src.bronze_noaa_station import settings
from src.managers.bigquery_manager import BigQueryManager

logger = logging.getLogger(__name__)
storage_client = storage.Client()
bucket = storage_client.bucket(settings.BUCKET)
bigquery_manager = BigQueryManager(project=settings.PROJECT, dataset=settings.DATASET)


def load_bronze_precip_raw_noaa_station_observations(
    blob_name: str,
) -> LoadJob:
    """
    Load raw NOAA station observations into the bronze BigQuery table.

    This function takes a raw JSON response from the NOAA API and loads it into
    BigQuery with the schema: _record_id, raw_response_json, _job_id, _trace_id,
    _record_created_at, _record_updated_at.

    Args:
        bigquery_manager: An instance of BigQueryManager for performing operations
        table: The name of the BigQuery table to load data into (not a fully qualified table path)
        blob_name: Name of the GCS blob containing raw NOAA station observations

    Returns:
        LoadJob: The BigQuery load job

    Raises:
        ValueError: If raw_response is empty or invalid
        google.cloud.exceptions.GoogleCloudError: If BigQuery operation fails
    """
    job_id = str(uuid.uuid4())

    # extract records from GCS blob
    logger.info(f"Reading observations from GCS blob: {blob_name}")
    blob = bucket.blob(blob_name)
    raw_response_str = blob.download_as_string()
    raw_response = json.loads(raw_response_str)

    # extract relevant content from response
    trace_id = raw_response["trace_id"]
    raw_records = raw_response["observations"]["features"]

    total_records_loaded = 0

    records = [
        {
            "_record_id": str(uuid.uuid4()),
            "raw_response_json": record,
            "_job_id": job_id,
            "_trace_id": trace_id,
            # INSERT statements won't honor default timestamps, so we need to set these here
            "_record_created_at": datetime.now(timezone.utc).isoformat(),
            "_record_updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for record in raw_records
    ]

    load_job = bigquery_manager.load_from_json(
        records=records,
        table=settings.TABLE,
        write_disposition="WRITE_APPEND",
    )

    total_records_loaded += load_job.output_rows
    logger.info(f"Successfully completed load total_records={total_records_loaded}")
