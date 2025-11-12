import asyncio
import logging
from statistics import mean
from typing import Any, Dict, List, Optional, TypedDict

from src.app.services.s3_service_async import AsyncS3Service

logger = logging.getLogger(__name__)


class WeatherData(TypedDict, total=False):
    temp_min: Optional[float]
    temp_max: Optional[float]
    temp_avg: Optional[float]
    humidity: Optional[float]
    precipitation: float
    wind_speed: Optional[float]


def parse_weather(data: Dict[str, Any]) -> WeatherData:
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
            for key in metrics:
                value = w.get(key)
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
