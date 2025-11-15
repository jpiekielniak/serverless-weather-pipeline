from typing import Tuple
from unittest.mock import Mock as _Mock

import pytest

from src.app.services.open_weather_map_api_client_async import WeatherService


def test_build_weather_url_includes_lat_lon_and_api_key() -> None:
    # Arrange
    svc = WeatherService("https://api.example.com", "key123")

    # Act
    url = svc.build_weather_url(12.34, 56.78)

    # Assert
    assert "lat=12.34" in url
    assert "lon=56.78" in url
    assert "appid=key123" in url


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_weather_by_coordinates_success_uses_session_mock(
    aiohttp_client_session_mock: Tuple[_Mock, _Mock],
) -> None:
    # Arrange
    session_obj, response = aiohttp_client_session_mock
    expected = {"temp": 20}
    # response.json is an AsyncMock in the fixture
    response.json.return_value = expected

    svc = WeatherService("https://api.example.com", "key123")

    # Act
    data = await svc.get_weather_by_coordinates(1.0, 2.0)

    # Assert
    assert data == expected
