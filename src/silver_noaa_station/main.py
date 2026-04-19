import uuid

from src.logging.custom_logger import get_logger
from src.managers.bigquery_manager import BigQueryManager
from src.silver_noaa_station import settings
from src.silver_noaa_station.transform import transform_fact_noaa_station_observations

logger = get_logger()
silver_bigquery_manager = BigQueryManager(
    project=settings.PROJECT, dataset=settings.SILVER_DATASET
)
bronze_bigquery_manager = BigQueryManager(
    project=settings.PROJECT, dataset=settings.BRONZE_DATASET
)


def main(trace_id):
    job_id = str(uuid.uuid4())

    logger.info(
        f"Starting Precipitation Silver Layer Transformations (job_id={job_id})"
    )

    target_table = silver_bigquery_manager._get_fully_qualified_table_path(
        "fact_noaa_station_precipitation"
    )

    source_table = bronze_bigquery_manager._get_fully_qualified_table_path(
        "raw_noaa_station_observations"
    )
    query_job = transform_fact_noaa_station_observations(
        target_table=target_table,
        source_table=source_table,
        job_id=job_id,
        trace_id=trace_id,
        bigquery_manager=silver_bigquery_manager,
    )

    logger.info(
        f"Completed Precipitation Silver Layer Transformations (job_id={job_id}, "
        f"num_records_affected={query_job.num_dml_affected_rows})"
    )


if __name__ == "__main__":
    main(trace_id=str(uuid.uuid4()))
