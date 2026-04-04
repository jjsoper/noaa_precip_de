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

    def get_observations(
        self, station_id: str, start: str = None, end: str = None, limit: int = None
    ) -> dict:
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if limit:
            params["limit"] = limit
        response = self.get(f"/stations/{station_id}/observations", params=params)
        return response

    def get_stations(
        self, id: str = None, state: str = None, limit: int = 500, cursor: str = None
    ) -> dict:
        params = {"limit": limit}
        if id:
            params["id"] = id
        if state:
            params["state"] = state
        if cursor:
            params["cursor"] = cursor
        response = self.get("/stations", params=params)
        return response
