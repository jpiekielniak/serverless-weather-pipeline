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
    tasks = [
        process_city_weather(city, weather_service, s3_service)
        for city in cities
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results


async def async_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
    return asyncio.run(async_handler(event, context))
