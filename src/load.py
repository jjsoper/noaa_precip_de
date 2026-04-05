import json
import uuid
from datetime import datetime
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJob, QueryJob

from src import settings
from src.logging.custom_logger import get_logger

logger = get_logger()


class BigQueryManager:
    """
    Reusable manager for BigQuery operations supporting multiple projects and datasets.

    Supports:
    - Reading from tables (queries, direct access)
    - Writing to tables (insert, load from JSON, load from file)
    - Table management (create, delete, describe)
    - Cross-project and cross-dataset operations
    """

    def __init__(
        self,
        client: bigquery.Client = None,
        default_project: str = None,
        default_dataset: str = None,
    ):
        """
        Initialize the BigQuery manager.

        Args:
            client: Optional BigQuery client. If not provided, creates a new one.
            default_project: Default project for operations. Defaults to settings.PROJECT.
            default_dataset: Default dataset for operations. Defaults to settings.DATASET.
        """
        self.client = client or bigquery.Client(
            project=default_project or settings.PROJECT
        )
        self.default_project = default_project or settings.PROJECT
        self.default_dataset = default_dataset or settings.DATASET

    def _get_table_id(
        self, table: str, dataset: str = None, project: str = None
    ) -> str:
        """
        Construct a fully qualified table ID.

        Args:
            table: Table name
            dataset: Dataset name. Defaults to default_dataset.
            project: Project ID. Defaults to default_project.

        Returns:
            Fully qualified table ID (project.dataset.table)
        """
        dataset = dataset or self.default_dataset
        project = project or self.default_project
        return f"{project}.{dataset}.{table}"

    def query(
        self,
        sql: str,
        project: str = None,
        job_config: bigquery.QueryJobConfig = None,
    ) -> QueryJob:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string
            project: Project ID. Defaults to default_project.
            job_config: Optional QueryJobConfig for advanced options.

        Returns:
            QueryJob: The BigQuery query job

        Raises:
            google.cloud.exceptions.GoogleCloudError: If query execution fails
        """
        logger.info(f"Executing query in project {project or self.default_project}")
        query_job = self.client.query(sql, job_config=job_config)
        query_job.result()
        logger.info("Query completed successfully")
        return query_job

    def read_table(
        self,
        table: str,
        dataset: str = None,
        project: str = None,
        max_results: int = None,
    ) -> bigquery.table.RowIterator:
        """
        Read data from a BigQuery table.

        Args:
            table: Table name
            dataset: Dataset name. Defaults to default_dataset.
            project: Project ID. Defaults to default_project.
            max_results: Maximum number of rows to return.

        Returns:
            RowIterator: Iterator over table rows

        Raises:
            google.cloud.exceptions.GoogleCloudError: If table access fails
        """
        table_id = self._get_table_id(table, dataset, project)
        logger.info(f"Reading data from {table_id}")
        return self.client.list_rows(table_id, max_results=max_results)

    def load_from_json(
        self,
        records: list[dict[str, Any]],
        table: str,
        dataset: str = None,
        project: str = None,
        write_disposition: str = "WRITE_APPEND",
        autodetect: bool = False,
    ) -> LoadJob:
        """
        Load records from JSON list into a BigQuery table.

        Args:
            records: List of dictionaries to load
            table: Target table name
            dataset: Target dataset. Defaults to default_dataset.
            project: Target project. Defaults to default_project.
            write_disposition: How to handle existing data. Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY.
            autodetect: Whether to auto-detect schema.

        Returns:
            LoadJob: The BigQuery load job

        Raises:
            ValueError: If records list is empty
            google.cloud.exceptions.GoogleCloudError: If load operation fails
        """
        if not records:
            raise ValueError("records list cannot be empty")

        table_id = self._get_table_id(table, dataset, project)

        write_disp_map = {
            "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
        }

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=autodetect,
            write_disposition=write_disp_map.get(
                write_disposition, bigquery.WriteDisposition.WRITE_APPEND
            ),
        )

        logger.info(
            f"Loading {len(records)} records into {table_id} "
            f"(write_disposition={write_disposition})"
        )

        load_job = self.client.load_table_from_json(
            records,
            table_id,
            job_config=job_config,
        )

        load_job.result()

        logger.info(f"Successfully loaded {load_job.output_rows} rows into {table_id}")

        return load_job

    def load_from_file(
        self,
        file_path: str,
        table: str,
        dataset: str = None,
        project: str = None,
        source_format: str = "JSON",
        write_disposition: str = "WRITE_APPEND",
        autodetect: bool = False,
    ) -> LoadJob:
        """
        Load data from a file into a BigQuery table.

        Args:
            file_path: Path to the file to load
            table: Target table name
            dataset: Target dataset. Defaults to default_dataset.
            project: Target project. Defaults to default_project.
            source_format: File format. Options: JSON, CSV, PARQUET, AVRO, ORC.
            write_disposition: How to handle existing data. Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY.
            autodetect: Whether to auto-detect schema.

        Returns:
            LoadJob: The BigQuery load job

        Raises:
            FileNotFoundError: If file does not exist
            google.cloud.exceptions.GoogleCloudError: If load operation fails
        """
        table_id = self._get_table_id(table, dataset, project)

        format_map = {
            "JSON": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "CSV": bigquery.SourceFormat.CSV,
            "PARQUET": bigquery.SourceFormat.PARQUET,
            "AVRO": bigquery.SourceFormat.AVRO,
            "ORC": bigquery.SourceFormat.ORC,
        }

        write_disp_map = {
            "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
        }

        job_config = bigquery.LoadJobConfig(
            source_format=format_map.get(
                source_format, bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            ),
            autodetect=autodetect,
            write_disposition=write_disp_map.get(
                write_disposition, bigquery.WriteDisposition.WRITE_APPEND
            ),
        )

        logger.info(
            f"Loading file {file_path} into {table_id} "
            f"(format={source_format}, write_disposition={write_disposition})"
        )

        with open(file_path, "rb") as f:
            load_job = self.client.load_table_from_file(
                f,
                table_id,
                job_config=job_config,
            )

        load_job.result()

        logger.info(f"Successfully loaded {load_job.output_rows} rows into {table_id}")

        return load_job

    def insert_rows(
        self,
        records: list[dict[str, Any]],
        table: str,
        dataset: str = None,
        project: str = None,
    ) -> list[dict]:
        """
        Insert rows into a BigQuery table (streaming insert).

        Args:
            records: List of row dictionaries
            table: Target table name
            dataset: Target dataset. Defaults to default_dataset.
            project: Target project. Defaults to default_project.

        Returns:
            List of error dictionaries (empty if successful)

        Raises:
            google.cloud.exceptions.GoogleCloudError: If insert operation fails
        """
        table_id = self._get_table_id(table, dataset, project)
        table_obj = self.client.get_table(table_id)

        logger.info(f"Inserting {len(records)} rows into {table_id}")

        errors = self.client.insert_rows_json(table_obj, records)

        if errors:
            logger.error(f"Errors occurred during insert: {errors}")
        else:
            logger.info(f"Successfully inserted {len(records)} rows into {table_id}")

        return errors

    def describe_table(
        self,
        table: str,
        dataset: str = None,
        project: str = None,
    ) -> bigquery.Table:
        """
        Get table metadata and schema.

        Args:
            table: Table name
            dataset: Dataset name. Defaults to default_dataset.
            project: Project ID. Defaults to default_project.

        Returns:
            Table: BigQuery Table object with metadata and schema

        Raises:
            google.cloud.exceptions.GoogleCloudError: If table access fails
        """
        table_id = self._get_table_id(table, dataset, project)
        logger.info(f"Fetching metadata for {table_id}")
        return self.client.get_table(table_id)

    def delete_table(
        self,
        table: str,
        dataset: str = None,
        project: str = None,
        not_found_ok: bool = True,
    ) -> None:
        """
        Delete a BigQuery table.

        Args:
            table: Table name
            dataset: Dataset name. Defaults to default_dataset.
            project: Project ID. Defaults to default_project.
            not_found_ok: If True, don't raise error if table doesn't exist.

        Raises:
            google.cloud.exceptions.NotFound: If table doesn't exist and not_found_ok=False
        """
        table_id = self._get_table_id(table, dataset, project)
        logger.info(f"Deleting table {table_id}")
        self.client.delete_table(table_id, not_found_ok=not_found_ok)
        logger.info(f"Successfully deleted {table_id}")


class BigQueryLoader:
    """
    Convenience wrapper for loading NOAA observation data into BigQuery.
    Uses BigQueryManager for underlying operations.
    """

    def __init__(self, manager: BigQueryManager = None, default_table: str = None):
        """
        Initialize the BigQuery loader.

        Args:
            manager: Optional BigQueryManager. If not provided, creates a new one.
            default_table: Default table name. Defaults to settings.TABLE.
        """
        self.manager = manager or BigQueryManager()
        self.default_table = default_table or settings.TABLE

    def load_observations(
        self,
        raw_response: dict[str, Any],
        table: str = None,
        dataset: str = None,
        project: str = None,
        job_id: str = None,
        event_id: str = None,
    ) -> LoadJob:
        """
        Load NOAA observation data into BigQuery.

        This method takes a raw JSON response from the NOAA API and loads it into
        BigQuery with the schema: _record_id, raw_response_json, _job_id, _event_id,
        _record_created_at, _record_updated_at.

        Args:
            raw_response: Raw JSON response from the NOAA API containing observations
            table: Target table. Defaults to default_table.
            dataset: Target dataset. Defaults to manager's default_dataset.
            project: Target project. Defaults to manager's default_project.
            job_id: Optional job ID. If not provided, a UUID is generated.
            event_id: Optional event ID. If not provided, a UUID is generated.

        Returns:
            LoadJob: The BigQuery load job

        Raises:
            ValueError: If raw_response is empty or invalid
            google.cloud.exceptions.GoogleCloudError: If BigQuery operation fails
        """
        if not raw_response:
            raise ValueError("raw_response cannot be empty")

        job_id = job_id or str(uuid.uuid4())
        event_id = event_id or str(uuid.uuid4())
        table = table or self.default_table

        # Prepare records from the raw response
        records = self._prepare_records(
            raw_response=raw_response,
            job_id=job_id,
            event_id=event_id,
        )

        if not records:
            logger.warning(
                f"No records to load for job_id={job_id}, event_id={event_id}"
            )
            return None

        return self.manager.load_from_json(
            records=records,
            table=table,
            dataset=dataset,
            project=project,
            write_disposition="WRITE_APPEND",
            autodetect=False,
        )

    def _prepare_records(
        self,
        raw_response: dict[str, Any],
        job_id: str,
        event_id: str,
    ) -> list[dict[str, Any]]:
        """
        Prepare records from raw NOAA API response for BigQuery insertion.

        The schema expects:
        - _record_id: unique identifier
        - raw_response_json: the raw JSON response
        - _job_id: data ingestion job ID
        - _event_id: event that triggered the job
        - _record_created_at: record creation timestamp
        - _record_updated_at: record update timestamp

        Args:
            raw_response: Raw JSON response from NOAA API
            job_id: ID of the data ingestion job
            event_id: ID of the event that triggered the job

        Returns:
            List of records ready for BigQuery insertion
        """
        records = []
        now = datetime.utcnow().isoformat() + "Z"

        # Handle the case where the response contains an array of observations
        observations = []
        if isinstance(raw_response, dict):
            if "features" in raw_response:
                # NOAA API returns observations in the "features" array
                observations = raw_response.get("features", [])
            else:
                # If it's a single observation object, wrap it in a list
                observations = [raw_response]
        elif isinstance(raw_response, list):
            observations = raw_response

        for observation in observations:
            if not observation:
                continue

            record = {
                "_record_id": str(uuid.uuid4()),
                "raw_response_json": json.dumps(observation),
                "_job_id": job_id,
                "_event_id": event_id,
                "_record_created_at": now,
                "_record_updated_at": now,
            }
            records.append(record)

        return records


def load_noaa_observations(
    raw_response: dict[str, Any],
    job_id: str = None,
    event_id: str = None,
    bigquery_client: bigquery.Client = None,
) -> LoadJob:
    """
    Convenience function to load NOAA observations into BigQuery.

    Args:
        raw_response: Raw JSON response from the NOAA API
        job_id: Optional job ID. If not provided, a UUID is generated.
        event_id: Optional event ID. If not provided, a UUID is generated.
        bigquery_client: Optional BigQuery client. If not provided, creates a new one.

    Returns:
        LoadJob: The BigQuery load job

    Raises:
        ValueError: If raw_response is empty or invalid
        google.cloud.exceptions.GoogleCloudError: If BigQuery operation fails
    """
    manager = BigQueryManager(client=bigquery_client)
    loader = BigQueryLoader(manager=manager)
    return loader.load_observations(
        raw_response=raw_response,
        job_id=job_id,
        event_id=event_id,
    )
