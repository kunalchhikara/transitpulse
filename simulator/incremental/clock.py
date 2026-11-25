"""Injectable clocks and timestamp formatting for the DE-11 loader.

The loader reads time only through a `Clock` (`now()` + `sleep()`), so audit
timestamps, Bronze `ingestion_time` and retry backoff are deterministic in
tests and reproducible runs, and no test ever really sleeps.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


def require_aware(value: datetime, what: str = "timestamp") -> datetime:
    """Return `value` in UTC; reject naive datetimes."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{what} must be a timezone-aware datetime, got {value!r}")
    return value.astimezone(UTC)


def parse_aware(text: str, what: str = "timestamp") -> datetime:
    """Parse ISO-8601 text that carries a UTC offset (`Z` or `+hh:mm`)."""
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{what} {text!r} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{what} {text!r} has no UTC offset (add Z or +hh:mm)")
    return parsed.astimezone(UTC)


def format_ts(value: datetime) -> str:
    """Canonical UTC text used in control/audit files (microseconds, `Z`)."""
    return require_aware(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def compact_ts(value: datetime) -> str:
    """Run-id-safe UTC text, e.g. `20250106T000000Z` / `20250106T000000.000001Z`."""
    utc = require_aware(value)
    fraction = f".{utc.microsecond:06d}" if utc.microsecond else ""
    return f"{utc:%Y%m%dT%H%M%S}{fraction}Z"


class SystemClock:
    """Wall clock; `sleep` really sleeps. Used by default outside tests."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class ManualClock:
    """Virtual clock: `now()` stands still until `sleep()`/`advance()` moves it.
    Never blocks. Every sleep is recorded in `sleeps` (for backoff tests).
    Also what the CLI's `--fixed-clock` uses, so reproducible runs are
    byte-identical even when a retry backs off."""

    def __init__(self, start: datetime) -> None:
        self._now = require_aware(start, "clock start")
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot sleep a negative duration")
        self.sleeps.append(seconds)
        self._now += timedelta(seconds=seconds)

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
