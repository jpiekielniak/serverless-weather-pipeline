import asyncio
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from src.app.services.db_service_async import AsyncDBService
from src.app.services.s3_service_async import AsyncS3Service

logger = logging.getLogger(__name__)


async def save_weather_aggregate(
    db_service: AsyncDBService,
    city_id: int,
    target_date: date,
    agg: Dict[str, Any],
) -> None:
    """Persist a daily aggregate into the database if not already present.

    The function checks whether an aggregate for `(city_id, target_date)`
    exists. If not, it inserts a new `WeatherAggregate` row using values from
    `agg`.

    Args:
        db_service (AsyncDBService): Database service used for queries.
        city_id (int): ID of the city in the database.
        target_date (date): Day for which the data is aggregated.
        agg (Dict[str, Any]): Mapping of aggregate metrics (e.g. temp_min,
            temp_max, temp_avg, humidity_avg, precipitation_sum, etc.).

    Raises:
        Exception: Propagates unexpected database errors after logging.
    """
    try:
        existing = await db_service.get_weather_aggregate(city_id, target_date)
        if not existing:
            await db_service.add_weather_aggregate(
                city_id=city_id, target_date=target_date, agg=agg
            )
            logger.info(
                f"🆕 Inserted new aggregate for city_id={city_id}, "
                f"date={target_date}"
            )

    except Exception as e:
        logger.exception(
            f"❌ Error saving weather aggregate for city_id={city_id}: {e}"
        )
        raise


async def save_processed_payload(
    s3_service: AsyncS3Service,
    city_name: str,
    target_date: date,
    agg: Dict[str, Any],
    day_files: List[str],
) -> str:
    """Write a processed JSON payload to S3 and return the object key.

    The payload includes the city name, aggregation date, computed aggregates
    and metadata about source files. Objects are written under:
    `processed/{city}/{YYYY}/{MM}/{DD}/{city}_{timestampZ}.json`.

    Args:
        s3_service (AsyncS3Service): S3 helper used to upload the payload.
        city_name (str): Human-readable city name.
        target_date (date): Date the aggregation represents.
        agg (Dict[str, Any): Aggregated metrics to store.
        day_files (List[str]): Source S3 keys used to compute the aggregate.

    Returns:
        str: The S3 object key (not a full s3:// URI) of the stored payload.

    Raises:
        ValueError: If `agg` is empty or `day_files` is empty.
        Exception: Propagates unexpected storage errors after logging.
    """
    if not agg:
        raise ValueError("Aggregation data is empty.")
    if not day_files:
        raise ValueError("No source files provided for aggregation.")

    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime("%Y%m%dT%H%M%SZ")

    processed_key = (
        Path("processed")
        / city_name
        / f"{target_date.year}"
        / f"{target_date.month:02}"
        / f"{target_date.day:02}"
        / f"{city_name}_{timestamp}.json"
    )

    payload = {
        "city": city_name,
        "date": str(target_date),
        "aggregates": agg,
        "metadata": {
            "source_files": len(day_files),
            "file_names": day_files,
            "timestamp_utc": utc_now.isoformat(),
        },
    }

    try:
        await s3_service.put_json(str(processed_key), payload)
        logger.info(
            f"📦 Saved processed payload for {city_name} → {processed_key}"
        )
        return str(processed_key)
    except Exception as e:
        logger.exception(
            f"❌ Failed to save processed payload for {city_name}: {e}"
        )
        raise


async def process_city_aggregate(
    db_service: AsyncDBService,
    s3_service: AsyncS3Service,
    city_id: int,
    city_name: str,
    target_date: date,
    agg: Dict[str, Any],
    day_files: List[str],
) -> Dict[str, Any]:
    """Persist aggregate data for a single city and return a summary.

    This function saves the aggregate to the database (if new) and writes a
    processed payload to S3. It returns a compact result dictionary suitable
    for responses and logging. Any exception is caught and returned as an
    error entry.

    Args:
        db_service (AsyncDBService): Database layer.
        s3_service (AsyncS3Service): S3 storage helper.
        city_id (int): Database city ID.
        city_name (str): Human-readable city name used in S3 paths.
        target_date (date): Date to which the aggregate applies.
        agg (Dict[str, Any]): Aggregated metrics.
        day_files (List[str]): Source S3 object keys used to compute
        the aggregate.

    Returns:
        Dict[str, Any]: On success: {
            "city": str,
            "date": str(YYYY-MM-DD),
            "processed_file": str (S3 object key),
            "records_used": int
        }.
        On failure: {"city": str, "error": str}.
    """
    try:
        await save_weather_aggregate(db_service, city_id, target_date, agg)
        processed_key = await save_processed_payload(
            s3_service, city_name, target_date, agg, day_files
        )

        return {
            "city": city_name,
            "date": str(target_date),
            "processed_file": processed_key,
            "records_used": agg.get("readings_count", 0),
        }

    except Exception as e:
        logger.exception(f"Error processing city {city_name}: {e}")
        return {"city": city_name, "error": str(e)}


async def process_all_cities(
    db_service: AsyncDBService,
    s3_service: AsyncS3Service,
    city_tasks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Run processing for all provided city tasks concurrently.

    Each element of `city_tasks` must be a dictionary containing:
        - city_id (int)
        - city_name (str)
        - target_date (date)
        - agg (Dict[str, Any])
        - day_files (List[str])

    Args:
        db_service (AsyncDBService): Database layer used by each task.
        s3_service (AsyncS3Service): S3 helper used by each task.
        city_tasks (List[Dict[str, Any]]): Per-city task descriptors.

    Returns:
        List[Dict[str, Any]]: List of per-city results. Failed tasks are
        logged and omitted from the returned list.
    """
    tasks = [
        asyncio.create_task(
            process_city_aggregate(
                db_service,
                s3_service,
                city["city_id"],
                city["city_name"],
                city["target_date"],
                city["agg"],
                city["day_files"],
            )
        )
        for city in city_tasks
    ]

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: List[Dict[str, Any]] = []
    for r in raw_results:
        if isinstance(r, BaseException):
            logger.error(f"City processing failed: {r}")
        else:
            results.append(r)

    logger.info(f"Processed {len(results)} cities successfully.")
    return results
