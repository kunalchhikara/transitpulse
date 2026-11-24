"""The real-time-paced (or fast-forwarded) emission loop.

`run()` drives a `TelemetryGenerator` at its configured `events_per_second`
using a fixed-rate scheduler (each tick's wake time is computed from the
run's start time, so pacing does not drift under per-iteration overhead),
calling `sink(event)` for each produced event and checking `should_stop()`
*before* producing the next event -- so a stop request (e.g. a SIGINT
handler flipping a flag) can only ever end the run between two complete,
already-fully-written events, never mid-event. See
`simulator/generator/cli.py` for how SIGINT/SIGTERM wire into `should_stop`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from simulator.generator.clock import Clock
from simulator.generator.core import TelemetryGenerator
from simulator.schemas.telemetry import TelemetryEvent

EventSink = Callable[[TelemetryEvent], None]


@dataclass
class RunSummary:
    events_emitted: int
    elapsed_seconds: float
    stopped_early: bool


def run(
    generator: TelemetryGenerator,
    sink: EventSink,
    *,
    clock: Clock,
    max_events: int | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> RunSummary:
    """Run `generator`, calling `sink` once per produced event, paced at
    `generator.generator_config.events_per_second` using `clock`.

    Stops when `max_events` is reached (if given) or `should_stop()`
    returns True (checked at the top of every iteration, before the next
    event is produced). Returns a `RunSummary` with the actual counts the
    caller can use to verify the achieved emission rate.
    """
    interval = 1.0 / generator.generator_config.events_per_second
    stop_check = should_stop or (lambda: False)

    start = clock.monotonic()
    next_tick_at = start
    tick = 0

    while (max_events is None or tick < max_events) and not stop_check():
        event = generator.produce_event(tick)
        sink(event)
        tick += 1

        next_tick_at += interval
        wait = next_tick_at - clock.monotonic()
        if wait > 0:
            clock.sleep(wait)

    elapsed = clock.monotonic() - start
    return RunSummary(
        events_emitted=tick,
        elapsed_seconds=elapsed,
        stopped_early=stop_check(),
    )
