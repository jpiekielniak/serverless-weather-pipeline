import asyncio
import logging
import os
import sys
from typing import Any, Callable, Dict, Generator, Tuple
from unittest.mock import AsyncMock, Mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="session")  # type: ignore[misc]
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Provide a dedicated asyncio event loop for the entire test session.

    Yields:
        asyncio.AbstractEventLoop: A fresh event loop that is closed after the
        test session completes.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def configure_logging() -> None:
    """Configure root logging for tests if not already set up.

    Side effects:
        Ensures DEBUG level logging is configured once for the session.
    """
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=logging.DEBUG)


def _make_mock_s3() -> Mock:
    """Create a generic S3 mock with common async methods stubbed.

    Returns:
        Mock: Object exposing async methods put_json, get_json, list_folders,
        and list_objects suitable for tests without real I/O.
    """
    s = Mock()
    s.put_json = AsyncMock(return_value=None)
    s.get_json = AsyncMock(return_value={})
    s.list_folders = AsyncMock(return_value=[])
    s.list_objects = AsyncMock(return_value=[])
    return s


@pytest.fixture  # type: ignore[misc]
def mock_db() -> Mock:
    """Provide a database service mock with common async methods.

    Returns:
        Mock: Object with async CRUD-like methods used by tests.
    """
    m = Mock()
    m.get_all_cities_with_coordinates = AsyncMock(return_value=[])
    m.get_city_by_name = AsyncMock(return_value=None)
    m.get_weather_aggregate = AsyncMock(return_value=None)
    m.add_weather_aggregate = AsyncMock(return_value=None)
    return m


@pytest.fixture  # type: ignore[misc]
def mock_s3() -> Mock:
    """Provide a generic S3 service mock."""
    return _make_mock_s3()


@pytest.fixture  # type: ignore[misc]
def mock_s3_raw() -> Mock:
    """Provide the raw-bucket S3 mock."""
    return _make_mock_s3()


@pytest.fixture  # type: ignore[misc]
def mock_s3_processed() -> Mock:
    """Provide the processed-bucket S3 mock."""
    return _make_mock_s3()


@pytest.fixture  # type: ignore[misc]
def mock_weather_service() -> Mock:
    """Provide a weather API client mock with an async fetch method."""
    ws = Mock()
    ws.get_weather_by_coordinates = AsyncMock(return_value={})
    return ws


@pytest.fixture  # type: ignore[misc]
def mock_secrets_manager() -> Mock:
    """Provide a Secrets Manager service mock with async get_secret."""
    sm = Mock()
    sm.get_secret = AsyncMock(return_value={})
    return sm


@pytest.fixture  # type: ignore[misc]
def env_vars(monkeypatch: MonkeyPatch) -> Callable[[Dict[str, Any]], None]:
    """Fixture to set environment variables for the duration of a test.

    Args:
        monkeypatch (MonkeyPatch): Pytest monkeypatch fixture used internally.

    Returns:
        Callable[[Dict[str, Any]], None]: Function that accepts a mapping of
        names to values and sets them in os.environ for the test.
    """

    def _setter(mapping: Dict[str, Any]) -> None:
        for k, v in mapping.items():
            monkeypatch.setenv(k, str(v))

    return _setter


@pytest.fixture  # type: ignore[misc]
def aioboto3_session_mock(mocker: MockerFixture) -> Tuple[Mock, AsyncMock]:
    """Patch aioboto3.Session to return an async-capable mocked client.

    Returns:
        Tuple[Mock, AsyncMock]: (mock_session, mock_client) pair. The client
        supports async context management and common S3 methods used in tests.
    """
    mock_client = AsyncMock()
    mock_client.get_paginator = Mock()

    client_ctx = Mock()
    client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
    client_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_session = Mock()
    mock_session.client.return_value = client_ctx

    mocker.patch(
        "src.app.services.s3_service_async.aioboto3.Session",
        return_value=mock_session,
    )
    return mock_session, mock_client


@pytest.fixture  # type: ignore[misc]
def aiohttp_client_session_mock(mocker: MockerFixture) -> Tuple[Mock, Mock]:
    """Patch aiohttp.ClientSession to return a session with a mocked GET.

    Returns:
        Tuple[Mock, Mock]: (session_obj, response) where response.json is an
        AsyncMock and raise_for_status is a no-op.
    """
    session_obj = Mock()

    get_ctx = Mock()
    response = Mock()
    response.raise_for_status = Mock(return_value=None)
    response.json = AsyncMock()
    get_ctx.__aenter__ = AsyncMock(return_value=response)
    get_ctx.__aexit__ = AsyncMock(return_value=None)

    session_obj.get.return_value = get_ctx

    client_session_ctx = Mock()
    client_session_ctx.__aenter__ = AsyncMock(return_value=session_obj)
    client_session_ctx.__aexit__ = AsyncMock(return_value=None)

    mocker.patch(
        "src.app.services.open_weather_map_api_client_async"
        ".aiohttp.ClientSession",
        return_value=client_session_ctx,
    )

    return session_obj, response
