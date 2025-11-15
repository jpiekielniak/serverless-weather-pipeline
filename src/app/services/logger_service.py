import json
import logging
import os
import queue
from logging.handlers import QueueHandler, QueueListener
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """Logging formatter that outputs JSON-formatted records.

    The formatter serializes key information from LogRecord into a compact
    JSON string. It includes level, message, logger name and a timestamp. If
    exception information is present it is included under the `exception`
    key.

    This formatter is suitable for structured logging systems and log
    aggregators that expect JSON input.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


def get_logger(name: str = "app") -> logging.Logger:
    """Create a process-local logger which serializes records to JSON
    and uses a queue listener.

    This helper sets up a background QueueListener and a QueueHandler so that
    log emission is non-blocking and the final serialization to JSON happens
    in a single consumer thread.

    Args:
        name (str): Logger name (defaults to "app").

    Returns:
        logging.Logger: Configured logger instance with JSON formatting.
    """
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    queue_handler = QueueHandler(log_queue)
    listener = QueueListener(log_queue, stream_handler)
    listener.start()

    logger = logging.getLogger(name)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    logger.addHandler(queue_handler)
    logger.propagate = False

    return logger
