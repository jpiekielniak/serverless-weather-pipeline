import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def put_json(self, city: str, data: Dict[str, Any]) -> Optional[str]:
        tz = ZoneInfo("Europe/Warsaw")
        now = datetime.now(tz)
        timestamp = now.isoformat(timespec="seconds").replace(":", "-")
        key = (
            f"raw/{city}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{city}_{timestamp}.json"
        )
        s3 = boto3.client("s3")
        body = json.dumps(data, ensure_ascii=False)
        try:
            s3.put_object(
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
