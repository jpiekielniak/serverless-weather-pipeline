import logging

import pytest

from src.app.services.logger_service import JsonFormatter, get_logger


def test_json_formatter_with_message_formats_message_as_json() -> None:
    # Arrange
    fmt = JsonFormatter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )

    # Act
    out = fmt.format(record)

    # Assert
    assert '"message": "hello world"' in out


def test_get_logger_has_handlers_and_disables_propagation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange / Act
    logger = get_logger("test")

    # Assert
    assert logger.handlers
    assert logger.propagate is False
