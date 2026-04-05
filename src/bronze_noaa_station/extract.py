import logging
from typing import Any

from src.managers.noaa_api_manager import NOAAWeatherManager

logger = logging.getLogger(__name__)


def extract_noaa_observations(
    noaa_manager: NOAAWeatherManager, station_id: list[str], start: str, end: str
) -> dict[str, Any]:

    try:
        response = noaa_manager.fetch_observations(
            station_id=station_id,
            start=start,
            end=end,
        )
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching observations for station {id}: {e}")
