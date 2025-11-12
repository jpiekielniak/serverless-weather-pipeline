import json
import logging
import os
import queue
from logging.handlers import QueueHandler, QueueListener
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
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
