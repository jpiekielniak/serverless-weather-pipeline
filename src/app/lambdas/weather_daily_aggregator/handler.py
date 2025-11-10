import json
import logging
import os
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, List, cast

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, configure_mappers, sessionmaker

from src.app.models import City, WeatherAggregate
from src.app.services.s3_service import S3Service
from src.app.services.secrets_manager_service import SecretsManagerService

configure_mappers()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"{name} environment variable not set")
    return value


def get_db_session(db_url: str) -> Session:
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def parse_weather(data: Dict[str, Any]) -> Dict[str, float]:
    main = data.get("main", {})
    wind = data.get("wind", {})
    rain = data.get("rain", {})

    return {
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "temp_avg": main.get("temp"),
        "humidity": main.get("humidity"),
        "precipitation": rain.get("1h", 0.0),
        "wind_speed": wind.get("speed"),
    }


def aggregate_city_weather(
    city_name: str, day_files: List[str], s3_raw: S3Service
) -> Dict[str, Any]:
    temps_min, temps_max, temps_avg, humidities, precipitations, winds = (
        [],
        [],
        [],
        [],
        [],
        [],
    )

    for file_key in day_files:
        try:
            data = s3_raw.get_json(file_key)
            w = parse_weather(data)
            if w["temp_min"] is not None:
                temps_min.append(w["temp_min"])
            if w["temp_max"] is not None:
                temps_max.append(w["temp_max"])
            if w["temp_avg"] is not None:
                temps_avg.append(w["temp_avg"])
            if w["humidity"] is not None:
                humidities.append(w["humidity"])
            if w["precipitation"] is not None:
                precipitations.append(w["precipitation"])
            if w["wind_speed"] is not None:
                winds.append(w["wind_speed"])
        except Exception as e:
            logger.error(f"Error parsing file {file_key}: {e}")

    if not temps_avg:
        raise ValueError(f"No valid data for {city_name}")

    return {
        "temp_min": min(temps_min),
        "temp_max": max(temps_max),
        "temp_avg": mean(temps_avg),
        "humidity_avg": mean(humidities),
        "precipitation_sum": sum(precipitations),
        "wind_speed_avg": mean(winds),
        "readings_count": len(temps_avg),
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    raw_bucket = get_env_var("RAW_BUCKET_NAME")
    processed_bucket = get_env_var("PROCESSED_BUCKET_NAME")
    secret_name_db = get_env_var("SECRET_NAME_DB")

    s3_raw = S3Service(raw_bucket)
    s3_processed = S3Service(processed_bucket)

    secret_manager_service = SecretsManagerService()
    secrets_db = secret_manager_service.get_secret(secret_name_db)
    db_url = str(secrets_db["db_url"])
    session = get_db_session(db_url)

    try:
        today = datetime.now(timezone.utc).date()
        target_date = today - timedelta(days=1)

        if "date" in event:
            target_date = datetime.strptime(event["date"], "%Y-%m-%d").date()

        year, month, day = (
            target_date.year,
            f"{target_date.month:02}",
            f"{target_date.day:02}",
        )
        logger.info(f"Aggregating weather data for {target_date}")

        city_prefixes = s3_raw.list_folders(prefix="raw/")
        results = []

        for city_prefix in city_prefixes:
            city_name = city_prefix.strip("/").split("/")[-1]
            day_prefix = f"raw/{city_name}/{year}/{month}/{day}/"

            day_files = s3_raw.list_objects(prefix=day_prefix)
            if not day_files:
                logger.info(f"No files for {city_name} on {target_date}")
                continue

            logger.info(
                f"Processing {len(day_files)} files for {city_name} "
                f"on {target_date}"
            )

            agg = aggregate_city_weather(city_name, day_files, s3_raw)

            db_city = (
                session.query(City).filter(City.name == city_name).first()
            )
            if not db_city:
                logger.warning(f"City {city_name} not found in DB — skipping")
                continue

            existing = (
                session.query(WeatherAggregate)
                .filter_by(city_id=db_city.id, date=target_date)
                .first()
            )

            if existing:
                for key, value in agg.items():
                    setattr(existing, key, value)
            else:
                city_id: int = cast(int, cast(object, db_city.id))
                session.add(
                    WeatherAggregate(city_id=city_id, date=target_date, **agg)
                )

            session.commit()

            processed_key = (
                f"processed/{city_name}/{year}/{month}/{day}/"
                f"{city_name}_{target_date}.json"
            )
            processed_payload = {
                "city": city_name,
                "date": str(target_date),
                "aggregates": agg,
                "metadata": {
                    "source_files": len(day_files),
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                },
            }

            s3_processed.put_json(processed_key, processed_payload)
            logger.info(f"Saved processed file: {processed_key}")

            results.append(
                {
                    "city": city_name,
                    "date": str(target_date),
                    "processed_file": processed_key,
                    "records_used": agg["readings_count"],
                }
            )

        return {
            "statusCode": 200,
            "body": json.dumps(results, ensure_ascii=False),
        }

    except Exception as e:
        logger.error(f"Handler error: {e}")
        session.rollback()
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        session.close()
