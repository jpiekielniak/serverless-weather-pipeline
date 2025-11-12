import json
import logging
from functools import lru_cache
from typing import Any, Dict

import aioboto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AsyncSecretsManagerService:
    def __init__(self) -> None:
        self.session = aioboto3.Session()

    @lru_cache(maxsize=10)
    async def get_secret(self, secret_name: str) -> Dict[str, Any]:
        try:
            async with self.session.client("secretsmanager") as client:
                response = await client.get_secret_value(SecretId=secret_name)
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
