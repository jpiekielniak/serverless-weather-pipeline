import json
import logging
import os
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


def get_db_session() -> Session:
    db_url = get_env_var("DATABASE_URL")
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    bucket_name = get_env_var("RAW_BUCKET_NAME")
    secret_name = get_env_var("SECRET_NAME_API")
    api_url = get_env_var("API_URL")

    secret_manager_service = SecretsManagerService()
    secrets = secret_manager_service.get_secret(secret_name)
    api_key = secrets.get("openweathermap")
    if not api_key:
        raise ValueError("OpenWeatherMap API key not found in secrets")

    weather_service = WeatherService(api_url, api_key)
    s3_service = S3Service(bucket_name)

    session = get_db_session()

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
                s3_path = s3_service.put_json(name, weather_data)
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
