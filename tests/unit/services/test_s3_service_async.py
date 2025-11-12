import json
from typing import Any, AsyncGenerator, Tuple
from unittest.mock import AsyncMock, Mock

import pytest

from src.app.services.s3_service_async import AsyncS3Service


@pytest.mark.asyncio  # type: ignore[misc]
async def test_put_json_success_returns_s3_uri(
    aioboto3_session_mock: Tuple[AsyncMock, AsyncMock]
) -> None:
    # Arrange
    mock_session, mock_client = aioboto3_session_mock
    mock_client.put_object.return_value = None

    service = AsyncS3Service("my-bucket")

    # Act
    result = await service.put_json("path/to/key.json", {"a": 1})

    # Assert
    assert result == "s3://my-bucket/path/to/key.json"
    mock_client.put_object.assert_awaited_once()


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_json_success_reads_and_parses_body(
    aioboto3_session_mock: Tuple[AsyncMock, AsyncMock]
) -> None:
    # Arrange
    mock_session, mock_client = aioboto3_session_mock
    data = {"hello": "world"}

    mock_stream = AsyncMock()
    mock_stream.__aenter__.return_value = mock_stream
    mock_stream.__aexit__.return_value = None
    mock_stream.read.return_value = json.dumps(data).encode("utf-8")

    mock_client.get_object.return_value = {"Body": mock_stream}

    service = AsyncS3Service("my-bucket")

    # Act
    result = await service.get_json("path/to/key.json")

    # Assert
    assert result == data


@pytest.mark.asyncio  # type: ignore[misc]
async def test_list_folders_and_objects_return_keys(
    aioboto3_session_mock: Tuple[AsyncMock, AsyncMock]
) -> None:
    # Arrange
    mock_session, mock_client = aioboto3_session_mock

    async def paginate_folders(
        *args: object, **kwargs: object
    ) -> AsyncGenerator[dict[str, list[dict[str, str]]], Any]:
        yield {"CommonPrefixes": [{"Prefix": "a/"}, {"Prefix": "b/"}]}

    async def paginate_objects(
        *args: object, **kwargs: object
    ) -> AsyncGenerator[dict[str, list[dict[str, str]]], Any]:
        yield {"Contents": [{"Key": "k1"}, {"Key": "k2"}]}

    paginator = Mock()
    paginator.paginate = paginate_folders
    mock_client.get_paginator.return_value = paginator

    service = AsyncS3Service("my-bucket")

    # Act
    folders = await service.list_folders("prefix/")

    # Assert
    assert folders == ["a/", "b/"]

    # Arrange (switch paginator behavior)
    paginator.paginate = paginate_objects

    # Act
    objects = await service.list_objects("prefix/")

    # Assert
    assert objects == ["k1", "k2"]
