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
    Reusable manager for BigQuery operations.

    Supports:
    - Reading from tables (queries, direct access)
    - Writing to tables (insert, load from JSON, load from file)
    - Table management (create, delete, describe)
    """

    def __init__(
        self,
        client: bigquery.Client,
        dataset: str,
    ):
        """
        Initialize the BigQuery manager.

        Args:
            client: Optional BigQuery client. If not provided, creates a new one.
            project: Project ID for operations.
            dataset: Dataset name for operations.
        """
        self.client = client
        self.project = client.project
        self.dataset = dataset

    def _get_fully_qualified_table_path(self, table: str) -> str:
        """
        Construct a fully qualified table path.

        Args:
            table: Table name

        Returns:
            Fully qualified table path (project.dataset.table)
        """
        return f"{self.project}.{self.dataset}.{table}"

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
        logger.info(f"Executing query in project {self.project}")
        query_job = self.client.query(sql, job_config=job_config)
        query_job.result()  # wait for job to complete
        logger.info("Query completed successfully")
        return query_job

    def list_rows(
        self,
        table: str,
        max_rows: int = None,
    ):
        """
        Read data from a BigQuery table.

        Args:
            table: Table name
            max_rows: Maximum number of rows to return.

        Returns:
            RowIterator: Iterator over table rows

        Raises:
            google.cloud.exceptions.GoogleCloudError: If table access fails
        """
        table_path = self._get_fully_qualified_table_path(table)
        logger.info(f"Listing rows from {table_path}")
        return self.client.list_rows(table_path, max_results=max_rows)

    def load_from_json(
        self,
        records: list[dict[str, Any]],
        table: str,
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
            write_disposition: How to handle existing data. Options:
                WRITE_APPEND
                WRITE_TRUNCATE
                WRITE_EMPTY.
            autodetect: Whether to auto-detect schema.

        Returns:
            LoadJob: The BigQuery load job

        Raises:
            ValueError: If records list is empty
        """
        if not records:
            raise ValueError("records list cannot be empty")

        table_path = self._get_fully_qualified_table_path(table)

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
            f"Loading {len(records)} records into {table_path} "
            f"(write_disposition={write_disposition})"
        )

        load_job = self.client.load_table_from_json(
            records,
            table_path,
            job_config=job_config,
        )

        load_job.result()

        logger.info(
            f"Successfully loaded {load_job.output_rows} rows into {table_path}"
        )

        return load_job


if __name__ == "__main__":
    # Example usage
    bq_manager = BigQueryManager(
        client=bigquery.Client(project=settings.PROJECT),
        dataset=settings.DATASET,
    )

    table = "raw_noaa_station_observations"
    row_iterator = bq_manager.list_rows(table, max_rows=5)
    row_iterator
