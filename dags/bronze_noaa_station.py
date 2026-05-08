import uuid

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.definitions import get_current_context

from src.bronze_noaa_station.extract import extract_noaa_observations
from src.bronze_noaa_station.load import (
    load_bronze_precip_raw_noaa_station_observations,
)


# no start date because weather.gov API only goes back so far
@dag(dag_id="noaa_station")
def dag_generator():

    @task()
    def extract():

        trace_id = str(uuid.uuid4())
        station_id = "KBOS"
        return extract_noaa_observations(
            station_id=station_id,
            start="2026-04-28T17:40:00+00:00",
            end="2026-04-29T17:40:00+00:00",
            trace_id=trace_id,
        )

    @task()
    def load(blob_name: str):
        load_bronze_precip_raw_noaa_station_observations(blob_name=blob_name)

    blob_name = extract()
    load(blob_name)


dag = dag_generator()

if __name__ == "__main__":
    dag.test()
