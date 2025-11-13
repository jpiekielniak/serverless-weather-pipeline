import json
import logging
from functools import lru_cache
from typing import Any, Dict

import aioboto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AsyncSecretsManagerService:
    """Asynchronous wrapper around AWS Secrets Manager client.

    Provides a cached async method to retrieve and parse JSON secrets from
    AWS Secrets Manager. Results are cached to reduce remote calls.

    Attributes:
        session (aioboto3.Session): aioboto3 session used to create clients.
    """

    def __init__(self) -> None:
        self.session = aioboto3.Session()

    @lru_cache(maxsize=10)
    async def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Fetch and parse a secret stored in AWS Secrets Manager.

        The method reads the `SecretString` value and parses it as JSON. The
        parsed value must be a JSON object (dict) — otherwise a ValueError is
        raised.

        Args:
            secret_name (str): The name or ARN of the secret to retrieve.

        Returns:
            Dict[str, Any]: Parsed JSON object stored in the secret.

        Raises:
            ValueError: If the secret string is empty or does not contain a
                JSON object.
            botocore.exceptions.ClientError: If the AWS API call fails.
        """
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
