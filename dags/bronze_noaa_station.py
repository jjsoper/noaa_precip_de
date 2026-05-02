import uuid

import pendulum
from airflow.sdk import dag, task

from src.bronze_noaa_station.extract import extract_noaa_observations
from src.bronze_noaa_station.load import (
    load_bronze_precip_raw_noaa_station_observations,
)


@dag(
    dag_id="bronze_noaa_station",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
)
def dag_generator():

    task_id = str(uuid.uuid4())

    @task()
    def extract():
        station_id = "KBOS"
        return extract_noaa_observations(
            station_id=station_id,
            start="2026-04-28T17:40:00+00:00",
            end="2026-04-29T17:40:00+00:00",
            trace_id=task_id,
        )

    @task()
    def load(blob_name: str):
        load_bronze_precip_raw_noaa_station_observations(blob_name=blob_name)

    blob_name = extract()
    load(blob_name)


dag = dag_generator()


if __name__ == "__main__":
    dag.test()
