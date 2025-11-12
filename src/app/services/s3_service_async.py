import json
import logging
from typing import Any, Dict, List, Optional, cast

import aioboto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AsyncS3Service:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    async def put_json(self, key: str, data: Dict[str, Any]) -> Optional[str]:
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
