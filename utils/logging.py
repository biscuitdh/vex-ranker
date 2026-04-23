"""Logging helpers for the VEX monitoring agent."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field_name in ("collector", "event", "team", "match_key", "source", "error"):
            value = getattr(record, field_name, None)
            if value not in (None, ""):
                payload[field_name] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(log_dir: Path, level: str = "INFO") -> None:
    """Configure root logging for console and file output."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(JsonFormatter())
    console.setLevel(level.upper())

    log_path = log_dir / "monitor.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    file_handler.setLevel(level.upper())

    root.addHandler(console)
    root.addHandler(file_handler)
