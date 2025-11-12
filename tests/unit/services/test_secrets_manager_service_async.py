import json
from typing import Any

import pytest
from botocore.exceptions import ClientError
from pytest_mock import MockerFixture

from src.app.services.secrets_manager_service_async import (
    AsyncSecretsManagerService,
)


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_secret_returns_parsed_dict(mocker: MockerFixture) -> None:
    # Arrange
    fake_secret_name = "my/test/secret"
    fake_secret_value: dict[str, Any] = {
        "username": "admin",
        "password": "123",
    }

    mock_client = mocker.AsyncMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(fake_secret_value)
    }

    mock_context_manager = mocker.AsyncMock()
    mock_context_manager.__aenter__.return_value = mock_client
    mock_context_manager.__aexit__.return_value = None

    mock_session = mocker.Mock()
    mock_session.client.return_value = mock_context_manager

    mocker.patch(
        "src.app.services.secrets_manager_service_async.aioboto3.Session",
        return_value=mock_session,
    )

    service = AsyncSecretsManagerService()

    # Act
    secret = await service.get_secret(fake_secret_name)

    # Assert
    assert secret == fake_secret_value
    mock_client.get_secret_value.assert_awaited_once_with(
        SecretId=fake_secret_name,
    )


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_secret_raises_if_secret_string_empty(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_client = mocker.AsyncMock()
    mock_client.get_secret_value.return_value = {"SecretString": ""}

    mock_context_manager = mocker.AsyncMock()
    mock_context_manager.__aenter__.return_value = mock_client
    mock_context_manager.__aexit__.return_value = None

    mock_session = mocker.Mock()
    mock_session.client.return_value = mock_context_manager

    mocker.patch(
        "src.app.services.secrets_manager_service_async.aioboto3.Session",
        return_value=mock_session,
    )

    service = AsyncSecretsManagerService()

    # Act / Assert
    with pytest.raises(
        ValueError, match="SecretString for my/empty/secret is empty"
    ):
        await service.get_secret("my/empty/secret")


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_secret_raises_if_secret_string_not_dict(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_client = mocker.AsyncMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(["not", "a", "dict"])
    }

    mock_context_manager = mocker.AsyncMock()
    mock_context_manager.__aenter__.return_value = mock_client
    mock_context_manager.__aexit__.return_value = None

    mock_session = mocker.Mock()
    mock_session.client.return_value = mock_context_manager

    mocker.patch(
        "src.app.services.secrets_manager_service_async.aioboto3.Session",
        return_value=mock_session,
    )

    service = AsyncSecretsManagerService()

    # Act / Assert
    with pytest.raises(
        ValueError, match="SecretString for invalid/secret is not a dict"
    ):
        await service.get_secret("invalid/secret")


@pytest.mark.asyncio  # type: ignore[misc]
async def test_get_secret_raises_on_client_error(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_client = mocker.AsyncMock()
    mock_client.get_secret_value.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Not found",
            }
        },
        "get_secret_value",
    )

    mock_context_manager = mocker.AsyncMock()
    mock_context_manager.__aenter__.return_value = mock_client
    mock_context_manager.__aexit__.return_value = None

    mock_session = mocker.Mock()
    mock_session.client.return_value = mock_context_manager

    mocker.patch(
        "src.app.services.secrets_manager_service_async.aioboto3.Session",
        return_value=mock_session,
    )

    service = AsyncSecretsManagerService()

    # Act / Assert
    with pytest.raises(ClientError):
        await service.get_secret("missing/secret")
