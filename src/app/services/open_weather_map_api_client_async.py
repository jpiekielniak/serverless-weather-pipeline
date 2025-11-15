import ssl
from typing import Any, Dict
from urllib.parse import urlencode

import aiohttp
import certifi
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class WeatherService:
    """Async client for fetching current weather from OpenWeatherMap.

    This service builds request URLs and performs HTTP GET requests to the
    configured OpenWeatherMap API endpoint. It retries transient network
    errors using an exponential backoff strategy.

    Attributes:
        api_url (str): Base URL of the OpenWeatherMap API.
        api_key (str): API key used for authenticating requests.
    """

    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def build_weather_url(self, lat: float, lon: float) -> str:
        """Build the OpenWeatherMap "current weather" endpoint URL.

        Args:
            lat (float): Latitude of the location.
            lon (float): Longitude of the location.

        Returns:
            str: Fully formed URL ready to be fetched.
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": "metric",
            "lang": "en",
        }
        return f"{self.api_url}/weather?{urlencode(params)}"

    @retry(
        retry=retry_if_exception_type(aiohttp.ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )  # type: ignore[misc]
    async def get_weather_by_coordinates(
        self, lat: float, lon: float
    ) -> Dict[str, Any]:
        """Retrieve current weather data for the specified coordinates.

        The method performs an HTTPS GET request and returns the parsed JSON
        response as a dictionary. Transient HTTP/client errors are retried
        automatically (see retry decorator).

        Args:
            lat (float): Latitude of the target location.
            lon (float): Longitude of the target location.

        Returns:
            Dict[str, Any]: Parsed JSON response from OpenWeatherMap.

        Raises:
            aiohttp.ClientError: For network-related failures.
            ValueError: If the server returns a non-dictionary JSON payload.
            aiohttp.HttpProcessingError: For non-2xx HTTP responses.
        """
        url = self.build_weather_url(lat, lon)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context),
            timeout=timeout,
        ) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                if not isinstance(data, dict):
                    raise ValueError(
                        f"Expected dict from API, got {type(data)}"
                    )

                return data
