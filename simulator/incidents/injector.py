"""`IncidentInjector`: wraps a DE-02 `TelemetryGenerator`, applying DE-03's
configurable `PROJECT.md` section 13 incident scenarios to its otherwise-
deterministic output stream.

Composition, not forking: `IncidentInjector` never reimplements movement,
RNG-draw order, or event construction -- every event starts as exactly what
`TelemetryGenerator.produce_event(tick)` would have produced, called with
strictly increasing `tick` values exactly once each (preserving DE-02's own
determinism contract, which depends on its internal `random.Random` and
per-vehicle state being advanced in a fixed call order). `IncidentInjector`
duck-types the two members `simulator.generator.runner.run` actually uses
(`.generator_config`, `.produce_event(tick)`), so it can be passed to `run()`
in place of a plain `TelemetryGenerator` with no further change to that
module beyond the one additive `rate_multiplier` hook.

All incident *decisions* (whether something fires on a given tick, which
malformed mode, how much late-event skew, which history entry to replay,
where to release from the reorder buffer) are drawn from a single
`random.Random(incidents_config.seed)` instance owned by this class, kept
entirely separate from the generator's own RNG -- so enabling or disabling
any subset of scenarios never changes the underlying generator's output
content, only what this wrapper does to/with it. Draws happen in a fixed
per-tick order (congestion/breakdown/late -> malformed -> duplicate/
out-of-order) regardless of which scenarios are enabled, matching DE-02's
"fixed draw order" determinism pattern.

Per-event pipeline (`produce_event`):
    1. Pull the next underlying event (`_build_payload`):
       a. congestion field overrides (route-level, window-based)
       b. breakdown field overrides (vehicle-level, window-based; wins over
          congestion if a vehicle matches both)
       c. late-event timestamp skew (probabilistic)
       d. convert to a JSON-serializable dict and apply malformed-data
          corruption (probabilistic) -- *after* step (d) the payload may no
          longer be a structurally valid `TelemetryEvent` at all, which is
          the point (see `simulator/incidents/malformed.py`).
    2. Delivery-level transforms, which decide *which* payload is actually
       returned for this call (`_apply_out_of_order` then `_apply_duplicate`):
       e. out-of-order reorder-buffer push/pop (only if enabled)
       f. duplicate replay from recent history (only if enabled)

`rate_multiplier(tick)` is the separate, runner-level hook for the volume
spike scenario (PROJECT.md section 13 item 6) -- it does not go through
`produce_event` at all; see `simulator/generator/runner.py`'s "DE-03 hook
point" docstring note.
"""

from __future__ import annotations

from datetime import timedelta
from random import Random
from typing import Any

from simulator.generator.core import TelemetryGenerator
from simulator.incidents.config import IncidentsConfig
from simulator.incidents.logging_setup import get_incident_logger
from simulator.incidents.malformed import corrupt_payload
from simulator.schemas.telemetry import TelemetryEvent, VehicleStatus

EventPayload = dict[str, Any]


class IncidentInjector:
    """See module docstring."""

    def __init__(self, generator: TelemetryGenerator, incidents_config: IncidentsConfig) -> None:
        self.generator = generator
        self.incidents_config = incidents_config
        self._rng = Random(incidents_config.seed)
        self._logger = get_incident_logger()

        self._next_tick = 0
        self._next_rate_tick = 0
        self._history: list[EventPayload] = []
        self._malformed_mode_index = 0
        # Edge-triggered state per window-based scenario, so "fired"/
        # "recovered" are each logged exactly once per transition rather
        # than once per tick spent inside/outside the window.
        self._active_state: dict[str, bool] = {
            "congestion": False,
            "volume_spike": False,
            "breakdown": False,
        }

        self._reorder_buffer: list[EventPayload] = []
        if incidents_config.out_of_order.enabled:
            # Pre-fill the reorder buffer before the main run begins, so
            # every call to `produce_event` can both push a new payload in
            # and pop exactly one out -- no call ever needs to emit nothing
            # or emit two payloads at once. See the `OutOfOrderConfig`/
            # module docstrings. The `hold_ticks` events consumed here are
            # real, fully-built events (congestion/breakdown/late/malformed
            # already applied) -- they are not discarded, only reordered.
            for _ in range(incidents_config.out_of_order.hold_ticks):
                self._reorder_buffer.append(self._build_payload(self._next_tick))
                self._next_tick += 1

    @property
    def generator_config(self):
        """Delegates to the wrapped generator's config -- this is the
        attribute `simulator.generator.runner.run` reads to compute its base
        pacing interval, so it must be present for duck-typing to work."""
        return self.generator.generator_config

    # -- public API ---------------------------------------------------

    def produce_event(self, tick: int) -> EventPayload:
        """Produce one output payload for external call index `tick`
        (ignored for underlying-generator indexing -- see module docstring;
        used only for window/log bookkeeping where it is safe to do so,
        i.e. whenever out-of-order pre-fill offset is zero)."""
        payload = self._build_payload(self._next_tick)
        self._next_tick += 1

        payload = self._apply_out_of_order(payload, tick)
        payload = self._apply_duplicate(payload, tick)
        return payload

    def rate_multiplier(self, tick: int) -> float:
        """PROJECT.md section 13 item 6 (volume spike): the runner-level
        rate hook for `simulator.generator.runner.run`.

        Keyed by this injector's own monotonic call counter rather than the
        `tick` argument `run()` passes in (which restarts at 0 on every
        separate `run()` call) -- so the configured window is tracked
        correctly even across multiple `run()` invocations against the same
        injector, exactly like `produce_event`'s internal tick counter."""
        effective_tick = self._next_rate_tick
        self._next_rate_tick += 1
        cfg = self.incidents_config.volume_spike
        active = cfg.enabled and cfg.window.contains(effective_tick)
        self._log_transition("volume_spike", active, effective_tick, {"rate_multiplier": cfg.rate_multiplier})
        return cfg.rate_multiplier if active else 1.0

    def flush_pending(self) -> list[EventPayload]:
        """Drain and return any payloads still sitting in the out-of-order
        reorder buffer at the end of a bounded run (nothing is lost, only
        delayed -- call this after the main loop to emit the final
        `hold_ticks` events)."""
        drained = list(self._reorder_buffer)
        self._reorder_buffer.clear()
        return drained

    # -- generation-level transforms (operate on a TelemetryEvent) ---

    def _build_payload(self, tick: int) -> EventPayload:
        event = self.generator.produce_event(tick)
        event = self._apply_congestion(event, tick)
        event = self._apply_breakdown(event, tick)
        event = self._apply_late_event(event, tick)
        payload = event.model_dump(mode="json")
        payload = self._apply_malformed(payload, tick)
        return payload

    def _apply_congestion(self, event: TelemetryEvent, tick: int) -> TelemetryEvent:
        cfg = self.incidents_config.congestion
        active = cfg.enabled and cfg.window.contains(tick)
        self._log_transition("congestion", active, tick, {"route_id": cfg.route_id})
        if not active or event.route_id != cfg.route_id:
            return event
        new_speed = round(event.speed_kmh * cfg.speed_multiplier, 2)
        new_delay = event.delay_seconds + cfg.extra_delay_seconds
        new_status = (
            VehicleStatus.DELAYED
            if new_delay >= self.generator.generator_config.delayed_status_threshold_seconds
            else event.status
        )
        return event.model_copy(
            update={
                "speed_kmh": new_speed,
                "delay_seconds": new_delay,
                "status": new_status,
            }
        )

    def _apply_breakdown(self, event: TelemetryEvent, tick: int) -> TelemetryEvent:
        cfg = self.incidents_config.breakdown
        active = cfg.enabled and cfg.window.contains(tick)
        self._log_transition("breakdown", active, tick, {"vehicle_ids": cfg.vehicle_ids})
        if not active or event.vehicle_id not in cfg.vehicle_ids:
            return event
        return event.model_copy(update={"speed_kmh": 0.0, "status": VehicleStatus.BREAKDOWN})

    def _apply_late_event(self, event: TelemetryEvent, tick: int) -> TelemetryEvent:
        cfg = self.incidents_config.late_event
        if not cfg.enabled or self._rng.random() >= cfg.probability_per_tick:
            return event
        skew_seconds = self._rng.randint(cfg.min_skew_seconds, cfg.max_skew_seconds)
        self._logger.info(
            "late_event incident fired",
            extra={"scenario": "late_event", "tick": tick, "skew_seconds": skew_seconds},
        )
        return event.model_copy(update={"event_time": event.event_time - timedelta(seconds=skew_seconds)})

    def _apply_malformed(self, payload: EventPayload, tick: int) -> EventPayload:
        cfg = self.incidents_config.malformed
        if not cfg.enabled or self._rng.random() >= cfg.probability_per_tick:
            return payload
        mode = cfg.modes[self._malformed_mode_index % len(cfg.modes)]
        self._malformed_mode_index += 1
        self._logger.info("malformed incident fired", extra={"scenario": "malformed", "tick": tick, "mode": mode})
        return corrupt_payload(payload, mode)

    # -- delivery-level transforms (operate on a raw payload dict) ---

    def _apply_out_of_order(self, payload: EventPayload, tick: int) -> EventPayload:
        cfg = self.incidents_config.out_of_order
        if not cfg.enabled:
            return payload
        self._reorder_buffer.append(payload)
        triggered = self._rng.random() < cfg.probability_per_tick
        index = self._rng.randrange(len(self._reorder_buffer)) if triggered else 0
        released = self._reorder_buffer.pop(index)
        if index != 0:
            self._logger.info(
                "out_of_order incident fired",
                extra={
                    "scenario": "out_of_order",
                    "tick": tick,
                    "released_event_id": released.get("event_id"),
                    "newest_event_id": payload.get("event_id"),
                },
            )
        return released

    def _apply_duplicate(self, payload: EventPayload, tick: int) -> EventPayload:
        cfg = self.incidents_config.duplicate
        if not cfg.enabled:
            self._remember(payload)
            return payload
        triggered = self._rng.random() < cfg.probability_per_tick
        if triggered and self._history:
            replay = dict(self._history[self._rng.randrange(len(self._history))])
            self._logger.info(
                "duplicate incident fired",
                extra={"scenario": "duplicate", "tick": tick, "replayed_event_id": replay.get("event_id")},
            )
            self._remember(payload)
            return replay
        self._remember(payload)
        return payload

    def _remember(self, payload: EventPayload) -> None:
        self._history.append(dict(payload))
        window = self.incidents_config.duplicate.replay_window
        if len(self._history) > window:
            self._history.pop(0)

    # -- shared logging ------------------------------------------------

    def _log_transition(self, name: str, active: bool, tick: int, context: dict[str, Any]) -> None:
        was_active = self._active_state.get(name, False)
        if active and not was_active:
            self._logger.info(f"{name} incident fired", extra={"scenario": name, "tick": tick, **context})
        elif was_active and not active:
            self._logger.info(f"{name} incident recovered", extra={"scenario": name, "tick": tick})
        self._active_state[name] = active
