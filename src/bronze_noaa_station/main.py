import uuid

from src.bronze_noaa_station import extract, load
from src.logging.custom_logger import get_logger

logger = get_logger()


def main(station_id: str, start: str, end: str):
    trace_id = str(uuid.uuid4())
    logger.info(f"Starting Precipitation Data Extraction/Load (trace_id={trace_id})")
    blob_name = extract.extract_noaa_observations(
        station_id=station_id,
        start=start,
        end=end,
        trace_id=trace_id,
    )
    load.load_bronze_precip_raw_noaa_station_observations(blob_name=blob_name)


if __name__ == "__main__":
    test_payload = {
        "station_id": "KBOS",
        "start": "2026-04-24T17:40:00+00:00",
        "end": "2026-04-25T17:40:00+00:00",
    }
    main(**test_payload)
