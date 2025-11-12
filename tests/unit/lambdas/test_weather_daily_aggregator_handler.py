import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock

import pytest

from src.app.lambdas.weather_daily_aggregator import handler as w_handler


def test_parse_event_date_with_date_parses_iso_string_to_date() -> None:
    payload = {"date": "2021-02-03"}
    d = w_handler.parse_event_date(payload)
    assert isinstance(d, date)
    assert d == date(2021, 2, 3)


def test_parse_event_date_default_is_yesterday_utc() -> None:
    expected = datetime.now(timezone.utc).date() - timedelta(days=1)
    d = w_handler.parse_event_date({})
    assert d == expected


@pytest.mark.asyncio  # type: ignore[misc]
async def test_gather_city_tasks_returns_empty_when_no_objects(
    mock_db: Mock, mock_s3: Mock
) -> None:
    mock_s3.list_folders.return_value = ["raw/city1/"]
    mock_s3.list_objects.return_value = []

    mock_db.get_city_by_name.assert_not_awaited()

    res = await w_handler.gather_city_tasks(mock_db, mock_s3, date(2022, 1, 1))

    assert res == []
    mock_db.get_city_by_name.assert_not_awaited()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_gather_city_tasks_skips_missing_db_city(
    mock_db: Mock, mock_s3: Mock, mocker: Mock
) -> None:
    mock_s3.list_folders.return_value = ["raw/city2/"]
    mock_s3.list_objects.return_value = ["f1.json"]

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler."
        "aggregate_city_weather_async",
        return_value={"readings_count": 1},
    )

    mock_db.get_city_by_name.return_value = None

    res = await w_handler.gather_city_tasks(mock_db, mock_s3, date(2022, 1, 2))

    assert res == []
    mock_db.get_city_by_name.assert_awaited()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_gather_city_tasks_returns_task_list_when_successful(
    mock_db: Mock, mock_s3: Mock, mocker: Mock
) -> None:
    mock_s3.list_folders.return_value = ["raw/mycity/"]
    mock_s3.list_objects.return_value = ["f1.json", "f2.json"]

    agg: Dict[str, Any] = {"readings_count": 3}
    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler"
        ".aggregate_city_weather_async",
        return_value=agg,
    )

    class DummyCity:
        def __init__(self, id: int) -> None:
            self.id = id

    mock_db.get_city_by_name.return_value = DummyCity(42)

    res = await w_handler.gather_city_tasks(mock_db, mock_s3, date(2022, 5, 6))

    assert isinstance(res, list)
    assert len(res) == 1
    task = res[0]
    assert task["city_id"] == 42
    assert task["city_name"] == "mycity"
    assert task["agg"] == agg


@pytest.mark.asyncio  # type: ignore[misc]
async def test_async_handler_success_initialises_and_processes_no_tasks(
    mock_db: Mock, mock_s3_raw: Mock, mock_s3_processed: Mock, mocker: Mock
) -> None:
    async def fake_init_services() -> Tuple[Mock, Mock, Mock]:
        return mock_db, mock_s3_raw, mock_s3_processed

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler.init_services",
        new=fake_init_services,
    )

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler.parse_event_date",
        return_value=date(2022, 1, 1),
    )

    async def fake_gather(*args: Any, **kwargs: Any) -> List[Any]:
        return []

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler.gather_city_tasks",
        new=fake_gather,
    )

    async def fake_process_all(*args: Any, **kwargs: Any) -> List[Any]:
        return []

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler.process_all_cities",
        new=fake_process_all,
    )

    res = await w_handler.async_handler({"date": "2022-01-01"}, None)

    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    assert body == []


def test_handler_sync_invokes_async_and_returns_result(mocker: Mock) -> None:
    async def fake_async_handler(event: Any, context: Any) -> Dict[str, Any]:
        return {"statusCode": 200, "body": json.dumps([])}

    mocker.patch(
        "src.app.lambdas.weather_daily_aggregator.handler.async_handler",
        new=fake_async_handler,
    )

    resp = w_handler.handler({}, None)

    assert resp["statusCode"] == 200
    assert json.loads(resp["body"]) == []
