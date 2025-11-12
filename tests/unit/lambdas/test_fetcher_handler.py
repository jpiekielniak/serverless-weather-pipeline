import json
from typing import Any, Dict, Tuple
from unittest.mock import Mock

import pytest

from src.app.lambdas.fetcher import handler as f_handler


@pytest.mark.asyncio  # type: ignore[misc]
async def test_store_weather_in_s3_awaits_put_and_returns_s3_path(
    mock_s3: Mock,
) -> None:
    data: dict[str, Any] = {"temp": 10}
    mock_s3.put_json.return_value = "s3://bucket/key.json"

    path = await f_handler.store_weather_in_s3(mock_s3, "City", data)

    assert path == "s3://bucket/key.json"
    mock_s3.put_json.assert_awaited_once()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_process_city_weather_success_returns_city_and_s3_path(
    mock_weather_service: Mock, mock_s3: Mock
) -> None:
    mock_weather_service.get_weather_by_coordinates.return_value = {"a": 1}
    mock_s3.put_json.return_value = "s3://bucket/City.json"

    res = await f_handler.process_city_weather(
        ("City", 1.0, 2.0), mock_weather_service, mock_s3
    )

    assert res["city"] == "City"
    assert res["s3_path"] == "s3://bucket/City.json"


@pytest.mark.asyncio  # type: ignore[misc]
async def test_process_city_weather_handles_exception_and_returns_error(
    mock_weather_service: Mock, mock_s3: Mock
) -> None:
    mock_weather_service.get_weather_by_coordinates.side_effect = Exception(
        "boom"
    )

    res = await f_handler.process_city_weather(
        ("City", 1.0, 2.0), mock_weather_service, mock_s3
    )

    assert res["city"] == "City"
    assert "error" in res and isinstance(res["error"], str)


@pytest.mark.asyncio  # type: ignore[misc]
async def test_process_all_cities_returns_results_list(
    mock_weather_service: Mock, mock_s3: Mock
) -> None:
    mock_weather_service.get_weather_by_coordinates.return_value = {"a": 1}
    mock_s3.put_json.return_value = "s3://bucket/City.json"
    cities: list[Tuple[str, float, float]] = [
        ("City", 1.0, 2.0),
        ("City2", 3.0, 4.0),
    ]

    res = await f_handler.process_all_cities(
        cities, mock_weather_service, mock_s3
    )

    assert isinstance(res, list)
    assert len(res) == 2


@pytest.mark.asyncio  # type: ignore[misc]
async def test_async_handler_success_initialises_services_and_returns_200(
    mock_db: Mock, mock_s3: Mock, mock_weather_service: Mock, mocker: Mock
) -> None:
    mock_db.get_all_cities_with_coordinates.return_value = [("A", 1.0, 2.0)]

    async def fake_init_services() -> Tuple[Mock, Mock, Mock]:
        return mock_db, mock_s3, mock_weather_service

    mocker.patch(
        "src.app.lambdas.fetcher.handler.init_services", new=fake_init_services
    )

    res = await f_handler.async_handler({}, None)

    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    assert isinstance(body, list)


def test_handler_sync_invokes_async_and_returns_result(mocker: Mock) -> None:
    async def fake_async_handler(event: Any, context: Any) -> Dict[str, Any]:
        return {"statusCode": 200, "body": json.dumps([])}

    mocker.patch(
        "src.app.lambdas.fetcher.handler.async_handler", new=fake_async_handler
    )

    resp = f_handler.handler({}, None)

    assert resp["statusCode"] == 200
    assert json.loads(resp["body"]) == []
