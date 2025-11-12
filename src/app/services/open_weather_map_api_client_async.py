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
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def build_weather_url(self, lat: float, lon: float) -> str:
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
        url = self.build_weather_url(lat, lon)
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        ) as session:
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()

                if not isinstance(data, dict):
                    raise ValueError(
                        f"Expected dict from API, got {type(data)}"
                    )

                return data
