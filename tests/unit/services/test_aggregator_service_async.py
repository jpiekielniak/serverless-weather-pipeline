from datetime import date
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest

from src.app.services.aggregator_service_async import (
    process_all_cities,
    process_city_aggregate,
    save_processed_payload,
    save_weather_aggregate,
)


@pytest.mark.asyncio  # type: ignore[misc]
async def test_save_processed_payload_puts_json_and_returns_key(
    mock_db: Mock, mock_s3: Mock
) -> None:
    # Arrange
    mock_s3.put_json.return_value = None
    agg: Dict[str, Any] = {"readings_count": 5}
    day_files: List[str] = ["a.json"]

    # Act
    key = await save_processed_payload(
        mock_s3, "City", date(2020, 1, 1), agg, day_files
    )

    # Assert
    assert isinstance(key, str)
    assert key.startswith("processed/City/")
    mock_s3.put_json.assert_awaited()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_save_weather_aggregate_adds_new_aggregate_when_missing(
    mock_db: Mock,
) -> None:
    # Arrange
    mock_db.get_weather_aggregate.return_value = None

    # Act
    await save_weather_aggregate(
        mock_db, 1, date(2020, 1, 1), {"readings_count": 1}
    )

    # Assert
    mock_db.add_weather_aggregate.assert_awaited()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_process_city_aggregate_and_process_all_cities_return_results(
    mock_db: Mock, mock_s3: Mock
) -> None:
    # Arrange
    city_task: Dict[str, Any] = {
        "city_id": 1,
        "city_name": "City",
        "target_date": date(2020, 1, 1),
        "agg": {"readings_count": 2},
        "day_files": ["f1"],
    }

    mock_db.get_weather_aggregate.return_value = None
    mock_db.add_weather_aggregate.return_value = True
    mock_s3.put_json.return_value = "s3://bucket/processed/City/key.json"

    # Act
    res = await process_city_aggregate(
        mock_db,
        mock_s3,
        1,
        "City",
        date(2020, 1, 1),
        city_task["agg"],
        city_task["day_files"],
    )

    # Assert
    assert res["city"] == "City"

    # Act: process all cities
    res_all = await process_all_cities(mock_db, mock_s3, [city_task])

    # Assert
    assert isinstance(res_all, list)
    assert len(res_all) == 1
