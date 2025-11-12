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
