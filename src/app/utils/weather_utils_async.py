import asyncio
import logging
from statistics import mean
from typing import Any, Dict, List, Optional, TypedDict

from src.app.services.s3_service_async import AsyncS3Service

logger = logging.getLogger(__name__)


class WeatherData(TypedDict, total=False):
    """Typed dict representing parsed weather fields from the provider.

    Fields are optional because some provider responses may not include every
    metric.

    Keys:
        temp_min (Optional[float])
        temp_max (Optional[float])
        temp_avg (Optional[float])
        humidity (Optional[float])
        precipitation (float)
        wind_speed (Optional[float])
    """

    temp_min: Optional[float]
    temp_max: Optional[float]
    temp_avg: Optional[float]
    humidity: Optional[float]
    precipitation: float
    wind_speed: Optional[float]


def parse_weather(data: Dict[str, Any]) -> WeatherData:
    """Extract a normalized WeatherData mapping from raw provider JSON.

    Args:
        data (Dict[str, Any]): Raw JSON decoded from the weather provider.

    Returns:
        WeatherData: Normalized mapping with expected keys.
    """
    main = data.get("main", {})
    wind = data.get("wind", {})
    rain = data.get("rain", {})
    return WeatherData(
        temp_min=main.get("temp_min"),
        temp_max=main.get("temp_max"),
        temp_avg=main.get("temp"),
        humidity=main.get("humidity"),
        precipitation=rain.get("1h", 0.0) or 0.0,
        wind_speed=wind.get("speed"),
    )


async def aggregate_city_weather_async(
    city_name: str, day_files: List[str], s3_raw: AsyncS3Service
) -> Dict[str, Any]:
    """Aggregate daily metrics for a city from raw per-file weather data.

    This function concurrently fetches multiple raw JSON files from S3,
    parses them using `parse_weather` and computes aggregated statistics.

    Args:
        city_name (str): Human-readable city name used for error messages.
        day_files (List[str]): List of S3 object keys containing raw weather
        JSON for the day.
        s3_raw (AsyncS3Service): Initialized S3 helper used to fetch
        file contents.

    Returns:
        Dict[str, Any]: Mapping with aggregated fields suitable for persistence

    Raises:
        ValueError: When no valid readings were found for the provided files.
    """
    metrics: Dict[str, List[float]] = {
        "temp_min": [],
        "temp_max": [],
        "temp_avg": [],
        "humidity": [],
        "precipitation": [],
        "wind_speed": [],
    }

    async def process_file(file_key: str) -> None:
        try:
            data: Dict[str, Any] = await s3_raw.get_json(file_key)
            w = parse_weather(data)
            wd: Dict[str, Any] = dict(w)
            for key in metrics:
                value = wd.get(key)
                if isinstance(value, (int, float)):
                    metrics[key].append(float(value))
        except Exception as e:
            logger.error(f"Error parsing file {file_key}: {e}")

    await asyncio.gather(*(process_file(f) for f in day_files))

    if not metrics["temp_avg"]:
        raise ValueError(f"No valid data for {city_name}")

    return {
        "temp_min": min(metrics["temp_min"]),
        "temp_max": max(metrics["temp_max"]),
        "temp_avg": mean(metrics["temp_avg"]),
        "humidity_avg": mean(metrics["humidity"]),
        "precipitation_sum": sum(metrics["precipitation"]),
        "wind_speed_avg": mean(metrics["wind_speed"]),
        "readings_count": len(metrics["temp_avg"]),
    }
