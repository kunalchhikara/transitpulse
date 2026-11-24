"""Injectable time source for the generator's real-time emission pacing.

`SystemClock` paces using the real wall clock (`time.monotonic`/`time.sleep`)
for live/demo CLI runs. `FakeClock` advances a virtual counter instead of
sleeping, so a test can exercise the emission-rate pacing logic in the
runner (`simulator.generator.runner.run`) instantly and deterministically --
this story's test requirements explicitly call for a "rate test (synthetic
clock, not wall-clock sleeps)" rather than a real `time.sleep`-based test.

Note: this clock only paces the *real-time loop* -- it never affects the
*content* of a generated `TelemetryEvent` (event_time/generated_at are
derived from the deterministic virtual timeline in
`simulator.generator.core.TelemetryGenerator`, not from wall-clock reads),
so swapping clocks cannot change determinism of the emitted sequence.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """Minimal time-source interface the emission runner depends on."""

    def monotonic(self) -> float:
        """A monotonically increasing time value, in seconds."""
        ...

    def sleep(self, seconds: float) -> None:
        """Advance time by `seconds` (blocking for `SystemClock`, instant
        for `FakeClock`). No-op for a non-positive duration."""
        ...


class SystemClock:
    """Real wall-clock time source, for live/demo CLI runs."""

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class FakeClock:
    """A synthetic clock for tests: `sleep()` advances an internal counter
    rather than blocking, so a test measuring elapsed time via `monotonic()`
    observes an exact virtual duration with zero real wall-clock delay."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._now += seconds
