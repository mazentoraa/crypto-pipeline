"""
utils/logger.py
───────────────
Structured JSON logger used throughout the project.

Usage:
    from utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Message here", coin="bitcoin", price=45000)

Each log line is a valid JSON object, e.g.:
    {"timestamp": "2024-01-15T10:30:00", "level": "INFO",
     "logger": "ingestion.batch_ingest", "message": "Fetched data",
     "coin": "bitcoin", "price": 45000}
"""

import json
import logging
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Merge any extra keyword arguments passed to the log call
        extra: dict = getattr(record, "extra_fields", {})
        log_data.update(extra)

        # Attach exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


class StructuredLogger:
    """
    Thin wrapper around the standard library logger that accepts
    arbitrary keyword arguments and merges them into the JSON output.
    """

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)

        # Only add handler once to avoid duplicate log lines
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(_JsonFormatter())
            self._logger.addHandler(handler)
            self._logger.propagate = False

    def _log(self, level: int, message: str, **kwargs) -> None:
        """Internal helper that injects keyword args as extra fields."""
        self._logger.log(level, message, extra={"extra_fields": kwargs})

    def debug(self, message: str, **kwargs) -> None:
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        self._log(logging.CRITICAL, message, **kwargs)


def get_logger(name: str) -> StructuredLogger:
    """
    Factory function — call this at the top of every module.

    Example:
        logger = get_logger(__name__)
    """
    return StructuredLogger(name)