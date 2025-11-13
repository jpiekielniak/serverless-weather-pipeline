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
    """Determine the aggregation date from the event or default to yesterday.

    If the `event` includes a `date` key, it must be formatted as
    YYYY-MM-DD and will be parsed accordingly. Otherwise, the function
    returns yesterday's date in UTC.

    Args:
        event (Dict[str, Any]): Lambda event which may contain a `date` string.

    Returns:
        date: Target date for which the aggregation should run.

    Raises:
        ValueError: If the provided `date` string cannot be parsed.
    """
    today = datetime.now(timezone.utc).date()
    if "date" in event:
        return datetime.strptime(event["date"], "%Y-%m-%d").date()
    return today - timedelta(days=1)


async def init_services() -> (
    Tuple[AsyncDBService, AsyncS3Service, AsyncS3Service]
):
    """Initialize DB and S3 services used by the aggregator.

    Reads configuration from environment variables and Secrets Manager, then
    constructs services for database access and S3 operations.

    Returns:
        Tuple[AsyncDBService, AsyncS3Service, AsyncS3Service]:
            Database service, raw S3 helper, and processed S3 helper.

    Raises:
        KeyError: If required keys are missing in the fetched secret.
        ValueError: If any environment variable is missing or invalid.
    """
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
    """Build task descriptors with per-city input for aggregation.

    This function inspects the raw S3 bucket to find cities and day files for
    the `target_date`. For each city with data, it computes a daily aggregate
    using `aggregate_city_weather_async` and prepares a task descriptor
    including the DB city id, computed aggregate and list of contributing
    files.

    Args:
        db_service (AsyncDBService): Service used to look up city ids by name.
        s3_raw (AsyncS3Service): Helper to list folders and files in the raw
        bucket.
        target_date (date): Date to aggregate.

    Returns:
        List[Dict[str, Any]]: List of per-city task objects with keys:
            - city_id (int)
            - city_name (str)
            - target_date (date)
            - agg (Dict[str, Any])
            - day_files (List[str])
    """
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
    """Asynchronous Lambda handler that runs the daily aggregation flow.

    Steps:
      1) Initialize services
      2) Compute `target_date` (from event or default)
      3) Gather per-city tasks
      4) Process all cities and return per-city results

    Args:
        event (Dict[str, Any]): Lambda event payload; may contain a `date`.
        context (Any): Lambda context object (unused).

    Returns:
        Dict[str, Any]: Lambda proxy response with `statusCode`
        and JSON `body`.
    """
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
    """Synchronous Lambda entrypoint that bridges to the async handler.

    Args:
        event (Dict[str, Any]): Lambda event payload.
        context (Any): Lambda context.

    Returns:
        Dict[str, Any]: Lambda proxy response created by `async_handler`.
    """
    return asyncio.run(async_handler(event, context))
