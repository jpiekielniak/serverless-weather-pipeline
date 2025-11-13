import json
import logging
from typing import Any, Dict, List, Optional, cast

import aioboto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AsyncS3Service:
    """Asynchronous helper for common S3 operations (JSON objects).

    This service provides convenience methods to put/get JSON objects and
    list keys using aioboto3. Methods return native Python types and handle
    ClientError logging.

    Attributes:
        bucket_name (str): Name of the S3 bucket to operate on.

    Example:
        async def main():
            svc = AsyncS3Service("my-bucket")
            await svc.put_json("path/to/key.json", {"a": 1})

        # Then run: asyncio.run(main())
    """

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    async def put_json(self, key: str, data: Dict[str, Any]) -> Optional[str]:
        """Upload a JSON-serializable mapping to S3 and return its s3 URI.

        Args:
            key (str): Object key within the bucket.
            data (Dict[str, Any]): JSON-serializable object to store.

        Returns:
            Optional[str]: s3:// URI on success,
            or None if the operation failed.
        """
        body = json.dumps(data, ensure_ascii=False)
        try:
            async with aioboto3.Session().client("s3") as s3:
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=body.encode("utf-8"),
                    ContentType="application/json",
                )
            logger.info(f"✅ Saved object to s3://{self.bucket_name}/{key}")
            return f"s3://{self.bucket_name}/{key}"
        except ClientError as e:
            logger.error(f"❌ Error saving object {key}: {e}")
            return None

    async def get_json(self, key: str) -> Dict[str, Any]:
        """Retrieve and parse a JSON object stored in S3.

        Args:
            key (str): Object key within the bucket.

        Returns:
            Dict[str, Any]: Parsed JSON content.

        Raises:
            botocore.exceptions.ClientError: If the S3 API call fails.
        """
        try:
            async with aioboto3.Session().client("s3") as s3:
                response = await s3.get_object(
                    Bucket=self.bucket_name, Key=key
                )
                async with response["Body"] as stream:
                    content = await stream.read()
            data = json.loads(content.decode("utf-8"))
            return cast(Dict[str, Any], data)
        except ClientError as e:
            logger.error(
                f"❌ Error getting object {key} from "
                f"bucket {self.bucket_name}: {e}"
            )
            raise

    async def list_folders(self, prefix: str) -> List[str]:
        """List folder prefixes under a given prefix in the bucket.

        Args:
            prefix (str): Prefix to search under.

        Returns:
            List[str]: Sorted list of folder prefix strings.
        """
        folders = set()
        try:
            async with aioboto3.Session().client("s3") as s3:
                paginator = s3.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
                ):
                    for common_prefix in page.get("CommonPrefixes", []):
                        folders.add(common_prefix["Prefix"])
            return sorted(folders)
        except ClientError as e:
            logger.error(f"❌ Error listing folders under {prefix}: {e}")
            return []

    async def list_objects(self, prefix: str) -> List[str]:
        """List object keys under a given prefix in the bucket.

        Args:
            prefix (str): Prefix to search under.

        Returns:
            List[str]: List of object keys found under `prefix`.
        """
        objects: List[str] = []
        try:
            async with aioboto3.Session().client("s3") as s3:
                paginator = s3.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self.bucket_name, Prefix=prefix
                ):
                    for obj in page.get("Contents", []):
                        objects.append(obj["Key"])
            return objects
        except ClientError as e:
            logger.error(f"❌ Error listing objects under {prefix}: {e}")
            return []
