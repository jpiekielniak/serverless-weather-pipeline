from typing import Any, Dict, cast
from urllib.parse import urlencode

import requests  # type: ignore[import-untyped]


class WeatherService:
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def get_weather_by_coordinates(
        self, lat: float, lon: float
    ) -> Dict[str, Any]:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": "metric",
            "lang": "en",
        }
        url = f"{self.api_url}/weather?{urlencode(params)}"

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict):
            raise ValueError(f"Expected dict from API, got {type(data)}")

        return cast(Dict[str, Any], data)
