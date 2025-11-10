import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, configure_mappers, sessionmaker

from src.app.models import Location
from src.app.models.city import City
from src.app.services.open_weather_map_api_client import WeatherService
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


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    bucket_name = get_env_var("RAW_BUCKET_NAME")
    secret_name_api = get_env_var("SECRET_NAME_API")
    secret_name_db = get_env_var("SECRET_NAME_DB")
    api_url = get_env_var("API_URL")

    secret_manager_service = SecretsManagerService()
    secrets_api = secret_manager_service.get_secret(secret_name_api)
    api_key = secrets_api.get("openweathermap")
    if not api_key:
        raise ValueError("OpenWeatherMap API key not found in secrets")

    weather_service = WeatherService(api_url, api_key)
    s3_service = S3Service(bucket_name)

    secrets_db = secret_manager_service.get_secret(secret_name_db)
    db_url = secrets_db.get("db_url")

    if not isinstance(db_url, str):
        raise ValueError("Database URL (db_url) missing or invalid in secrets")

    session = get_db_session(db_url)

    results = []

    try:
        cities = (
            session.query(City.name, Location.latitude, Location.longitude)
            .join(City.location)
            .all()
        )

        for name, lat, lon in cities:
            try:
                weather_data = weather_service.get_weather_by_coordinates(
                    lat, lon
                )
                now = datetime.now(timezone.utc)
                timestamp = now.isoformat(timespec="seconds").replace(":", "-")
                key = (
                    f"raw/{name}/{now.year}/{now.month:02d}/{now.day:02d}/"
                    f"{name}_{timestamp}.json"
                )

                s3_path = s3_service.put_json(key, weather_data)
                results.append({"city": name, "s3_path": s3_path})
                logger.info(f"Weather for {name} saved to {s3_path}")
            except Exception as e:
                logger.error(f"Error fetching weather for {name}: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps(results, ensure_ascii=False),
        }

    except Exception as e:
        logger.error(f"Handler error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        session.close()
