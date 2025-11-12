import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from src.app.services.aggregator_service_async import process_all_cities
from src.app.services.db_service_async import AsyncDBService
from src.app.services.logger_service import get_logger
from src.app.services.s3_service_async import AsyncS3Service
from src.app.services.secrets_manager_service_async import (
    AsyncSecretsManagerService,
)
from src.app.utils.common import get_env_var
from src.app.utils.weather_utils_async import aggregate_city_weather_async

logger = get_logger(__name__)


def parse_event_date(event: Dict[str, Any]) -> date:
    today = datetime.now(timezone.utc).date()
    if "date" in event:
        return datetime.strptime(event["date"], "%Y-%m-%d").date()
    return today - timedelta(days=1)


async def init_services() -> (
    Tuple[AsyncDBService, AsyncS3Service, AsyncS3Service]
):
    raw_bucket = get_env_var("RAW_BUCKET_NAME")
    processed_bucket = get_env_var("PROCESSED_BUCKET_NAME")
    secret_name_db = get_env_var("SECRET_NAME_DB")

    secret_manager = AsyncSecretsManagerService()
    secrets_db = await secret_manager.get_secret(secret_name_db)
    db_url = str(secrets_db["db_url"])

    db_service = AsyncDBService(db_url)
    s3_raw = AsyncS3Service(raw_bucket)
    s3_processed = AsyncS3Service(processed_bucket)

    logger.info("All services initialized successfully")
    return db_service, s3_raw, s3_processed


async def gather_city_tasks(
    db_service: AsyncDBService,
    s3_raw: AsyncS3Service,
    target_date: date,
) -> List[Dict[str, Any]]:
    city_prefixes: List[str] = await s3_raw.list_folders(prefix="raw/")
    city_tasks: List[Dict[str, Any]] = []

    for city_prefix in city_prefixes:
        city_name = city_prefix.strip("/").split("/")[-1]
        day_prefix = (
            f"raw/{city_name}/{target_date.year}/{target_date.month:02}/"
            f"{target_date.day:02}/"
        )
        day_files = await s3_raw.list_objects(prefix=day_prefix)

        if not day_files:
            logger.info(f"No files for {city_name} on {target_date}")
            continue

        agg = await aggregate_city_weather_async(city_name, day_files, s3_raw)

        db_city = await db_service.get_city_by_name(city_name)
        if not db_city:
            logger.warning(f"City {city_name} not found in DB — skipping")
            continue

        city_tasks.append(
            {
                "city_id": db_city.id,
                "city_name": city_name,
                "target_date": target_date,
                "agg": agg,
                "day_files": day_files,
            }
        )

    return city_tasks


async def async_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        db_service, s3_raw, s3_processed = await init_services()
        target_date = parse_event_date(event)
        logger.info(f"Aggregating weather data for {target_date}")

        city_tasks = await gather_city_tasks(db_service, s3_raw, target_date)

        results = await process_all_cities(
            db_service, s3_processed, city_tasks
        )

        return {
            "statusCode": 200,
            "body": json.dumps(results, ensure_ascii=False),
        }

    except Exception as e:
        logger.exception(f"Handler failed: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return asyncio.run(async_handler(event, context))
