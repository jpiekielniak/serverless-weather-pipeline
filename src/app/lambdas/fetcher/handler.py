import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import requests  # type: ignore[import-untyped]

from ...services.s3_service import put_json
from ...services.secrets_service import get_secret

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    city = event.get("city", "Warsaw")

    bucket_name = os.environ.get("RAW_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("RAW_BUCKET_NAME environment variable not set")

    secret_name = os.environ.get("SECRET_NAME_API")
    if not secret_name:
        raise ValueError("SECRET_NAME_API environment variable not set")

    secrets = get_secret(secret_name)
    api_key = secrets.get("openweathermap")

    base_url = os.environ.get("API_URL")
    url = f"{base_url}/weather?q={city}&appid={api_key}&units=metric&lang=en"
    logger.info(f"Fetching weather for city: {city}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%dT%H-%M-%SZ")
        key = (
            f"raw/{city}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{city}_{timestamp}.json"
        )

        put_json(bucket_name, key, data)
        logger.info(f"Saved data into s3://{bucket_name}/{key}")

        result = {"city": data["name"], "s3_path": f"s3://{bucket_name}/{key}"}

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Error HTTP: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
