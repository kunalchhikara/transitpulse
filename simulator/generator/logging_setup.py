"""Structured JSON-lines logging for the generator CLI.

One JSON object per log line (`PROJECT.md` section 19 / section 15
observability). Logs are written to stderr by default, or to a file via
`file_path` -- never mixed into the stdout event stream that
`simulator/generator/cli.py` writes `TelemetryEvent` JSON lines to.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

LOGGER_NAME = "simulator.generator"

# Standard `logging.LogRecord` attributes, used to separate caller-supplied
# `extra={...}` context from the record's built-in fields.
_STANDARD_RECORD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys())


class JsonLinesFormatter(logging.Formatter):
    """Renders one `logging.LogRecord` as a single JSON object, no newline
    embedded inside the payload itself (the handler appends exactly one)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRS and key not in payload
        }
        if context:
            payload["context"] = context
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, default=str)


def configure_logging(
    *,
    level: str = "INFO",
    stream: IO[str] | None = None,
    file_path: str | Path | None = None,
) -> logging.Logger:
    """Configure (and return) the `simulator.generator` logger to emit one
    JSON line per record.

    Exactly one destination is used: `file_path` if given, else `stream`
    (default `sys.stderr`). `propagate` is disabled so these records never
    duplicate onto the root logger / stdout.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    for existing in list(logger.handlers):
        logger.removeHandler(existing)

    if file_path is not None:
        handler: logging.Handler = logging.FileHandler(file_path, encoding="utf-8")
    else:
        handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonLinesFormatter())
    logger.addHandler(handler)
    return logger
