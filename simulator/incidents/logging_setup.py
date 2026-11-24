"""Structured JSON-lines logging for incident injection, reusing DE-02's
`JsonLinesFormatter` (`simulator/generator/logging_setup.py`) so incident
log lines have exactly the same shape (`timestamp`, `level`, `logger`,
`message`, optional `context`) as the generator's own logs.

A separate logger name (`simulator.incidents`, vs DE-02's
`simulator.generator`) is used -- `simulator.generator.logging_setup.
configure_logging` hardcodes the latter name, so it cannot be reused
directly for a second, independently-configurable logger; this module is
the small, additive counterpart for incidents, not a fork of that one.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import IO

from simulator.generator.logging_setup import JsonLinesFormatter

LOGGER_NAME = "simulator.incidents"


def configure_incident_logging(
    *,
    level: str = "INFO",
    stream: IO[str] | None = None,
    file_path: str | Path | None = None,
) -> logging.Logger:
    """Configure (and return) the `simulator.incidents` logger to emit one
    JSON line per record. Exactly one destination is used: `file_path` if
    given, else `stream` (default `sys.stderr`) -- never mixed into the
    stdout event stream."""
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


def get_incident_logger() -> logging.Logger:
    """Return the `simulator.incidents` logger, configuring it with sane
    defaults (INFO, stderr) if nothing has configured it yet -- so
    `IncidentInjector` always has somewhere to log even when a caller
    (e.g. a test) never calls `configure_incident_logging` explicitly."""
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        return configure_incident_logging()
    return logger
