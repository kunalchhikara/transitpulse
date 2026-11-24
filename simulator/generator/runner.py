"""The real-time-paced (or fast-forwarded) emission loop.

`run()` drives a `TelemetryGenerator` at its configured `events_per_second`
using a fixed-rate scheduler (each tick's wake time is computed from the
run's start time, so pacing does not drift under per-iteration overhead),
calling `sink(event)` for each produced event and checking `should_stop()`
*before* producing the next event -- so a stop request (e.g. a SIGINT
handler flipping a flag) can only ever end the run between two complete,
already-fully-written events, never mid-event. See
`simulator/generator/cli.py` for how SIGINT/SIGTERM wire into `should_stop`.

DE-03 hook point: `rate_multiplier`, below, is the one small, additive
change this story (incident injection, `simulator/incidents/`) makes to
DE-02's runtime. It defaults to `None` (every existing call site and test
is unaffected -- the computed interval is identical to before), and lets a
caller scale the pacing interval tick-by-tick, which is what
`simulator.incidents.injector.IncidentInjector`'s "volume spike" scenario
uses to temporarily emit at a configured multiple of the base rate without
`run()` needing to know anything about incidents. `generator` itself is
accessed here only via the two attributes/methods already used below
(`.generator_config.events_per_second`, `.produce_event(tick)`), so an
`IncidentInjector` -- which exposes the same two -- can be passed in its
place with no further change to this module.
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
    rate_multiplier: Callable[[int], float] | None = None,
) -> RunSummary:
    """Run `generator`, calling `sink` once per produced event, paced at
    `generator.generator_config.events_per_second` using `clock`.

    Stops when `max_events` is reached (if given) or `should_stop()`
    returns True (checked at the top of every iteration, before the next
    event is produced). Returns a `RunSummary` with the actual counts the
    caller can use to verify the achieved emission rate.

    `rate_multiplier`, if given, is called with the tick index that was
    just produced and must return a positive multiplier applied to the
    base interval for the gap *after* that tick (1.0 reproduces the
    unmodified base rate; `None`, the default, is equivalent to always
    returning 1.0 -- see the module docstring's "DE-03 hook point" note).
    """
    base_interval = 1.0 / generator.generator_config.events_per_second
    get_multiplier = rate_multiplier or (lambda _tick: 1.0)
    stop_check = should_stop or (lambda: False)

    start = clock.monotonic()
    next_tick_at = start
    tick = 0

    while (max_events is None or tick < max_events) and not stop_check():
        event = generator.produce_event(tick)
        sink(event)

        interval = base_interval / get_multiplier(tick)
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
