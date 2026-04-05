import json
import uuid

from flask import Flask, request

from src.bronze_noaa_station import extract, load, settings
from src.logging.custom_logger import get_logger
from src.managers.bigquery_manager import BigQueryManager
from src.managers.noaa_api_manager import NOAAWeatherManager

logger = get_logger()
bigquery_manager = BigQueryManager(project=settings.PROJECT, dataset=settings.DATASET)
noaa_manager = NOAAWeatherManager()

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main(test_payload: dict = None):
    event_id = str(uuid.uuid4())
    job_id = event_id  # for now, we will set these equal

    logger.info(f"Starting Precipitation Data Extraction/Load (job_id={job_id})")

    if test_payload:
        data = test_payload
    else:
        data = request.get_json()

    if not data:
        logger.error("No payload received")
        return {"error": "No payload received"}, 400

    logger.info(f"Received payload: {json.dumps(data)}")

    if "station_ids" not in data or "start" not in data or "end" not in data:
        m = "Missing required fields: station_ids, start, end"
        logger.error(m)
        return {"error": m}, 400

    try:
        total_records_loaded = 0
        for station_id in data["station_ids"]:
            logger.info(f"Extracting observations for station: {station_id}")
            noaa_response = extract.extract_noaa_observations(
                noaa_manager=noaa_manager,
                station_id=station_id,
                start=data["start"],
                end=data["end"],
            )

            with open(f"noaa_response_{station_id}.json", "w") as f:
                json.dump(noaa_response, f)

            if noaa_response:
                logger.info(f"Loading observations for station: {station_id}")
                raw_records = noaa_response["features"]
                table = "raw_noaa_station_observations"
                load_job = load.load_bronze_precip_raw_noaa_station_observations(
                    bigquery_manager=bigquery_manager,
                    table=table,
                    raw_records=raw_records,
                    job_id=job_id,
                    event_id=event_id,
                )

                total_records_loaded += load_job.output_rows

        logger.info(
            f"Successfully completed extraction/load "
            f"(job_id={job_id}, total_records={total_records_loaded})"
        )

        return {
            "status": "success",
            "job_id": job_id,
            "event_id": event_id,
            "total_records_loaded": total_records_loaded,
        }, 200

    except Exception as e:
        m = f"Error during data extraction/load: {e}"
        logger.error(m)
        return {"error": m}, 500


if __name__ == "__main__":
    test_payload = {
        "station_ids": ["KBOS"],
        "start": "2026-04-01T17:40:00+00:00",
        "end": "2026-04-02T17:40:00+00:00",
    }
    main(test_payload)
