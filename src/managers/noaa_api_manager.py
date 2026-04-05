from src.clients.client import Client


class NOAAWeatherManager(Client):
    """
    Thin manager for NOAA Weather API.
    """

    def __init__(
        self,
        **client_kwargs: dict[str, str],
    ) -> None:
        base_url = "https://api.weather.gov"
        super().__init__(base_url=base_url, **client_kwargs)

    def fetch_observations(
        self, station_id: str, start: str = None, end: str = None, limit: int = None
    ) -> dict:
        """Fetches observations for a given station.

        Args:
            station_id (str): The station identifier for the station to get observations for
            start (str, optional): The start timestamp (YYYY-MM-DDTHH:MM:SS+00:00)
                for the observation period. Defaults to None.
            end (str, optional): The end timestamp (YYYY-MM-DDTHH:MM:SS+00:00)
                for the observation period. Defaults to None.
            limit (int, optional): The maximum number of observations to return. Defaults to None.

        Returns:
            dict: A dictionary containing the observations for the specified station and time period.
        """
        params = {}
        path = f"/stations/{station_id}/observations"
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if limit:
            params["limit"] = limit
        response = self.get(path, params=params)
        return response

    def fetch_stations(
        self, id: str = None, state: str = None, limit: int = 500, cursor: str = None
    ) -> dict:
        """Fetches a list of stations

        Args:
            id (str, optional): The station ID. Defaults to None.
            state (str, optional): The state where the station is located. Defaults to None.
            limit (int, optional): The maximum number of stations to return. Defaults to 500.
            cursor (str, optional): The cursor for pagination. Defaults to None.

        Returns:
            dict: A dictionary containing the list of stations.
        """

        params = {"limit": limit}
        path = "/stations"
        if id:
            params["id"] = id
        if state:
            params["state"] = state
        if cursor:
            params["cursor"] = cursor
        response = self.get(path, params=params)
        return response
