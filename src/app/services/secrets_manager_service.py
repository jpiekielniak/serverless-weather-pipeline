import json
import logging
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class SecretsManagerService:
    def __init__(self) -> None:
        self.client = boto3.session.Session().client(
            service_name="secretsmanager"
        )

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            secret_string = response.get("SecretString")

            if not secret_string:
                raise ValueError(f"SecretString for {secret_name} is empty")

            secret_data = json.loads(secret_string)
            if not isinstance(secret_data, dict):
                raise ValueError(
                    f"SecretString for {secret_name} is not a dict"
                )

            return secret_data

        except ClientError as e:
            logger.error(
                f"Failed to get secret string from {secret_name}: {e}"
            )
            raise
