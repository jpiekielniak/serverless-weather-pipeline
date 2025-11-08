import json
import logging
from typing import Any, Dict

import boto3

logger = logging.getLogger(__name__)


def get_secret(secret_name: str) -> Dict[str, Any]:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString")

        if not secret_string:
            raise ValueError(f"SecretString for {secret_name} is empty")

        secret_data = json.loads(secret_string)

        if not isinstance(secret_data, dict):
            raise ValueError(f"SecretString for {secret_name} is not a dict")

        return secret_data

    except Exception as e:
        logger.error(f"Failed to get secret string from {secret_name}: {e}")
        raise
