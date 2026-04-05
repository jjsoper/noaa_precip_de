from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Client:
    """
    Synchronous HTTP client using requests with retries/backoff.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = None,
        user_agent: str = None,
        extra_headers: dict = {},
        timeout: float = 10.0,  # seconds
        retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        self.api_key = api_key
        self.user_agent = user_agent
        self.extra_headers = extra_headers

        retry_config = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT"]),
        )
        adapter = HTTPAdapter(max_retries=retry_config)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _build_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers.update("Authorization", f"Bearer {self.api_key}")
        if self.user_agent:
            headers.update("User-Agent", self.user_agent)
        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] = None,
        payload: dict[str, str] = None,
        headers: dict[str, str] = None,
        stream: bool = False,
    ) -> requests.Response:
        url = self._url(path)
        headers = self._build_headers()
        response = self.session.request(
            method,
            url,
            params=params,
            data=payload,
            headers=headers,
            timeout=self.timeout,
            stream=stream,
        )
        response.raise_for_status()
        return response

    def get(self, path: str, params: dict[str, str] = None) -> requests.Response:
        """Generic get request for API client

        Args:
            path (str): API URL
            params (dict[str, str]): Additional parameters to pass

        Returns:
            requests.Response: API response object
        """
        response = self._request("GET", path, params)
        return response

    def post(self, path: str, payload: dict[str, str]) -> requests.Response:
        """Generic post request for API client

        Args:
            path (str): API URL
            payload (dict[str, str]): Additional parameters to pass

        Returns:
            requests.Response: API response object
        """
        response = self._request("POST", path, payload=payload)
        return response
