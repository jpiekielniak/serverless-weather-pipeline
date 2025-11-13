import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.app.services.db_service_async import AsyncDBService
from src.app.services.logger_service import get_logger
from src.app.services.open_weather_map_api_client_async import WeatherService
from src.app.services.s3_service_async import AsyncS3Service
from src.app.services.secrets_manager_service_async import (
    AsyncSecretsManagerService,
)
from src.app.utils.common import get_env_var

logger = get_logger(__name__)


async def init_services() -> (
    Tuple[AsyncDBService, AsyncS3Service, WeatherService]
):
    """Initialize and return core async services used by the handler.

    This function reads configuration from environment variables and fetches
    secret values from AWS Secrets Manager. It constructs the database,
    storage and weather API services.

    Returns:
        Tuple[AsyncDBService, AsyncS3Service, WeatherService]:
            A tuple containing initialized DB, S3 and weather API services.

    Raises:
        ValueError: If required secrets or environment variables are missing
            or invalid (e.g., missing OpenWeatherMap API key or DB URL).
    """
    bucket_name = get_env_var("RAW_BUCKET_NAME")
    secret_name_api = get_env_var("SECRET_NAME_API")
    secret_name_db = get_env_var("SECRET_NAME_DB")
    api_url = get_env_var("API_URL")

    secret_manager = AsyncSecretsManagerService()

    secrets_api = await secret_manager.get_secret(secret_name_api)
    api_key = secrets_api.get("openweathermap")
    if not api_key:
        raise ValueError("OpenWeatherMap API key not found in secrets")

    weather_service = WeatherService(api_url, api_key)
    s3_service = AsyncS3Service(bucket_name)

    secrets_db = await secret_manager.get_secret(secret_name_db)
    db_url = secrets_db.get("db_url")
    if not isinstance(db_url, str):
        raise ValueError("Database URL (db_url) missing or invalid in secrets")

    db_service = AsyncDBService(db_url)

    logger.info("✅ All async services initialized successfully")
    return db_service, s3_service, weather_service


async def store_weather_in_s3(
    s3_service: AsyncS3Service, city_name: str, weather_data: Dict[str, Any]
) -> Optional[str]:
    """Persist a single city's weather JSON to S3 and return its URI.

    The object key is timestamped and organized by city and date so that files
    are easy to list and aggregate later.

    Args:
        s3_service (AsyncS3Service): Initialized S3 helper.
        city_name (str): Human-readable city name used in the S3 key.
        weather_data (Dict[str, Any]): JSON-serializable weather payload.

    Returns:
        Optional[str]: s3:// URI of the stored object on success,
        None on failure.
    """
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat(timespec="seconds").replace(":", "-")
    key = (
        f"raw/{city_name}/{now.year}/{now.month:02d}/{now.day:02d}/"
        f"{city_name}_{timestamp}.json"
    )

    s3_path = await s3_service.put_json(key, weather_data)
    logger.info(f"💾 Weather for {city_name} saved to {s3_path}")
    return s3_path


async def process_city_weather(
    city: Tuple[str, float, float],
    weather_service: WeatherService,
    s3_service: AsyncS3Service,
) -> Dict[str, Any]:
    """Fetch and store weather for a single city.

    Args:
        city (Tuple[str, float, float]): Tuple of (name, latitude, longitude).
        weather_service (WeatherService): Client for OpenWeatherMap API.
        s3_service (AsyncS3Service): S3 helper used to store raw JSON.

    Returns:
        Dict[str, Any]: A result mapping containing at least the `city` name
        and either `s3_path` on success or `error` on failure.
    """
    name, lat, lon = city
    try:
        weather_data = await weather_service.get_weather_by_coordinates(
            lat, lon
        )
        s3_path = await store_weather_in_s3(s3_service, name, weather_data)
        return {"city": name, "s3_path": s3_path}
    except Exception as e:
        logger.exception(f"❌ Error processing city {name}: {e}")
        return {"city": name, "error": str(e)}


async def process_all_cities(
    cities: list[tuple[str, float, float]],
    weather_service: WeatherService,
    s3_service: AsyncS3Service,
) -> List[Dict[str, Any]]:
    """Process a batch of cities concurrently.

    This function schedules per-city tasks and awaits them as a group.

    Args:
        cities (list[tuple[str, float, float]]): List of (name, lat, lon).
        weather_service (WeatherService): Client used by each task.
        s3_service (AsyncS3Service): S3 helper used by each task.

    Returns:
        List[Dict[str, Any]]: Per-city results; each contains `city` and
        either `s3_path` or `error`.
    """
    tasks = [
        process_city_weather(city, weather_service, s3_service)
        for city in cities
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results


async def async_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Asynchronous AWS Lambda handler entrypoint.

    Fetches the list of cities from the database, retrieves current weather
    for each city and stores raw JSON files in S3.

    Args:
        event (Dict[str, Any]): Lambda event payload (unused here).
        context (Any): Lambda context object (unused here).

    Returns:
        Dict[str, Any]: Lambda proxy response with `statusCode` and JSON `body`
    """
    try:
        db_service, s3_service, weather_service = await init_services()

        cities = await db_service.get_all_cities_with_coordinates()
        cities_tuples = [(name, lat, lon) for name, lat, lon in cities]
        logger.info(f"🌍 Fetched {len(cities_tuples)} cities from database")

        results = await process_all_cities(
            cities_tuples, weather_service, s3_service
        )

        logger.info(f"✅ Successfully processed {len(results)} cities")

        return {
            "statusCode": 200,
            "body": json.dumps(results, ensure_ascii=False),
        }
    except Exception as e:
        logger.exception(f"🔥 Handler failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Synchronous Lambda entrypoint that bridges to the async handler.

    Args:
        event (Dict[str, Any]): Lambda event payload.
        context (Any): Lambda context object.

    Returns:
        Dict[str, Any]: Lambda proxy response as produced by `async_handler`.
    """
    return asyncio.run(async_handler(event, context))
