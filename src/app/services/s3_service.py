import json
import logging
from typing import Any, Dict, List, Optional, cast

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def put_json(self, key: str, data: Dict[str, Any]) -> Optional[str]:
        try:
            body = json.dumps(data, ensure_ascii=False)
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            logger.info(f"Saved object to s3://{self.bucket_name}/{key}")
            return f"s3://{self.bucket_name}/{key}"
        except ClientError as e:
            logger.error(
                f"Error saving object to s3://{self.bucket_name}/{key}: {e}"
            )
            return None

    def get_json(self, key: str) -> Dict[str, Any]:
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            content = response["Body"].read().decode("utf-8")
            return cast(Dict[str, Any], json.loads(content))
        except ClientError as e:
            logger.error(
                f"Error getting object {key} from bucket {self.bucket_name}: "
                f"{e}"
            )
            raise

    def list_folders(self, prefix: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            folders = set()

            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
            ):
                for common_prefix in page.get("CommonPrefixes", []):
                    folders.add(common_prefix["Prefix"])

            return sorted(folders)
        except ClientError as e:
            logger.error(f"Error listing folders under {prefix}: {e}")
            return []

    def list_objects(self, prefix: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            objects = []

            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    objects.append(obj["Key"])

            return objects
        except ClientError as e:
            logger.error(f"Error listing objects under {prefix}: {e}")
            return []
