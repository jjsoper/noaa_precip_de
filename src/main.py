import json
import logging.config

from src import settings
from src.logging.custom_logger import get_logger
from src.managers.noaa_api_manager import NOAAWeatherManager

logger = get_logger()

noaa_manager = NOAAWeatherManager()


def main():
    logger.info("Starting Precip ETL")
    station = "KBOS"
    response = noaa_manager.get_observations(
        station_id=station,
        start="2026-04-01T17:40:00+00:00",
        end="2026-04-02T17:40:00+00:00",
    )

    output = [
        {
            "timestamp": obs["properties"]["timestamp"],
            "precipitationLastHour": obs["properties"]["precipitationLastHour"],
        }
        for obs in response.json()["features"]
        if obs["properties"].get("precipitationLastHour", None) is not None
    ]

    with open("observations.json", "w") as f:
        json.dump(output, f, indent=2)

    logger.info(output)


if __name__ == "__main__":
    main()

    # stations = noaa_manager.get_stations(limit=500, state="MA")
    # station_meta = stations.json()
    # logger.info(station_meta.keys())
    # for station in station_meta["features"]:
    #     name = station["properties"]["name"]
    #     id = station["properties"]["stationIdentifier"]
    #     logger.info(f"{name}: {id}")
