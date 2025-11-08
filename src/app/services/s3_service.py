import json
import logging
from typing import Any

import boto3

logger = logging.getLogger(__name__)


def put_json(
    bucket: str, key: str, payload: Any, content_type: str = "application/json"
) -> None:
    """Zapisuje obiekt JSON do S3 pod wskazanym kluczem.

    Podnosi wyjątek jeśli operacja się nie powiedzie.
    """
    s3 = boto3.client("s3")
    body = json.dumps(payload, ensure_ascii=False)
    try:
        s3.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType=content_type
        )
        logger.info("Zapisano obiekt do s3://%s/%s", bucket, key)
    except Exception as e:
        logger.error(
            "Błąd podczas zapisu do S3 s3://%s/%s: %s", bucket, key, e
        )
        raise
