"""DE-06 event-time state: the per-record watermark, and the DE-08 / BACKLOG
FU-06 STREAM-relative watermark guard (README "DE-06: Event time, watermark,
late and out-of-order policy" and "Watermark guard").

The guard is decided only from the stream of valid, first-seen events in
arrival order - never from a wall, ingestion or processing clock - so the
result is deterministic, replayable and independent of when or how the data
was ingested.

A valid, first-seen event E (event_time t, vehicle v) is a **jump** when no
watermark exists yet or `t > max_event_time + max_jump_seconds`. A jump is
**held back** (loaded, flagged `_silver_watermark_guarded`, classified as
usual, but it never advances the watermark) when, among the next
`lookahead_events` valid first-seen events in arrival order:

* fewer than `min_corroborating_vehicles` distinct vehicles other than v have
  an event with `event_time >= t - max_jump_seconds` (nobody else is "there"),
  AND
* events from at least `min_contradicting_vehicles` distinct vehicles (v
  included) have `event_time < t - max_jump_seconds` (the stream carries on
  far behind it).

Otherwise it advances the watermark exactly as without the guard. Because a
decision may need later events, records are released in arrival order only
once every earlier decision is final (a lookahead buffer: at most
`lookahead_events` valid events, plus any interleaved rejects), so output
order and every non-held event's flags are exactly those of the unguarded
watermark run on the stream without the held-back events.

Quarantined records whose event_time is usable are "probed": they get the
same verdict (for Gold attribution) but never move the watermark and are
never evidence for other decisions.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from simulator.silver.config import WatermarkGuardConfig, WatermarkGuardMode

#: `_silver_event_time_status` values of a clean record.
IN_ORDER = "in_order"
OUT_OF_ORDER = "out_of_order"
LATE = "late"


class EventTimeTracker:
    """Per-record event-time watermark over valid, first-seen events in
    arrival order: `watermark = max(event_time seen) - allowed_lateness`.

    * event_time >= max seen               -> in_order
    * watermark <= event_time < max seen   -> out_of_order (within lateness)
    * event_time < watermark               -> late
    Only events that pass validation and dedup advance the maximum (a
    corrupt future timestamp must not make everything after it "late"), and
    an event the DE-08 watermark guard holds back is observed with
    `advance=False`."""

    def __init__(self, allowed_lateness_seconds: int) -> None:
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_event_time: datetime | None = None
        self.min_event_time: datetime | None = None

    @property
    def watermark(self) -> datetime | None:
        return None if self.max_event_time is None else self.max_event_time - self.allowed_lateness

    def observe(self, event_time: datetime, *, advance: bool = True) -> tuple[str, float, datetime | None]:
        """Classify `event_time`, then advance (unless `advance` is False: a
        held-back event is classified against the current state but never
        moves the maximum). Returns (status, lateness_seconds behind the max
        seen so far, watermark at arrival)."""
        watermark = self.watermark
        if self.max_event_time is None or event_time >= self.max_event_time:
            status, lateness = IN_ORDER, 0.0
        else:
            lateness = (self.max_event_time - event_time).total_seconds()
            status = LATE if event_time < watermark else OUT_OF_ORDER
        if advance and (self.max_event_time is None or event_time > self.max_event_time):
            self.max_event_time = event_time
        if self.min_event_time is None or event_time < self.min_event_time:
            self.min_event_time = event_time
        return status, lateness, watermark


@dataclass(frozen=True)
class WatermarkGuard:
    """The stream-relative guard's decision rule (see module docstring)."""

    mode: WatermarkGuardMode
    max_jump_seconds: int
    lookahead_events: int
    min_corroborating_vehicles: int
    min_contradicting_vehicles: int

    @classmethod
    def from_config(cls, config: WatermarkGuardConfig) -> WatermarkGuard:
        return cls(config.mode, config.max_jump_seconds, config.lookahead_events,
                   config.min_corroborating_vehicles, config.min_contradicting_vehicles)

    @property
    def enabled(self) -> bool:
        return self.mode is not WatermarkGuardMode.OFF

    @property
    def max_jump(self) -> timedelta:
        return timedelta(seconds=self.max_jump_seconds)

    def is_jump(self, event_time: datetime, max_event_time: datetime | None) -> bool:
        return max_event_time is None or event_time > max_event_time + self.max_jump

    def decide(
        self, event_time: datetime, vehicle_id: str, later: list[tuple[datetime, str]], *, complete: bool
    ) -> bool | None:
        """For a jump: True = hold back, False = advance, None = undecided
        (needs more of `later`). `later` holds the following valid first-seen
        events (event_time, vehicle_id) in arrival order; `complete` means no
        more will come (end of input) or the lookahead is full. The answer is
        always the one the full lookahead window would give."""
        window = later[: self.lookahead_events]
        floor = event_time - self.max_jump
        corroborating: set[str] = set()
        contradicting: set[str] = set()
        for other_time, other_vehicle in window:
            if other_time >= floor:
                if other_vehicle != vehicle_id:
                    corroborating.add(other_vehicle)
            else:
                contradicting.add(other_vehicle)
        if len(corroborating) >= self.min_corroborating_vehicles:
            return False
        contradicted = len(contradicting) >= self.min_contradicting_vehicles
        remaining = 0 if complete else self.lookahead_events - len(window)
        if remaining == 0:
            return contradicted
        if contradicted and remaining < self.min_corroborating_vehicles - len(corroborating):
            return True  # can no longer be corroborated, and already contradicted
        return None


@dataclass(frozen=True)
class EventTimeDecision:
    status: str
    lateness_seconds: float
    watermark: datetime | None
    held_back: bool


#: Pending entry kinds.
_OTHER, _EVENT, _PROBE = "other", "event", "probe"


class EventTimeSequencer:
    """Releases records in arrival order, classifying each valid first-seen
    event once its watermark-guard decision is final. With the guard off (or
    no jump pending) nothing waits.

    * `event` - a valid first-seen event: classified, and advances the
      watermark unless held back. Only these are evidence for other decisions.
    * `probe` - a quarantined record whose event_time is usable: the same
      jump/hold decision is recorded for it (Gold uses it for attribution),
      but it never advances the watermark and is never evidence.
    * `other` - anything else (duplicates, unusable quarantined records):
      only kept in order."""

    def __init__(
        self,
        tracker: EventTimeTracker,
        guard: WatermarkGuard,
        emit_other: Callable[[Any], None],
        emit_event: Callable[[Any, EventTimeDecision], None],
        emit_probe: Callable[[Any, bool], None] | None = None,
    ) -> None:
        self.tracker = tracker
        self.guard = guard
        self._emit_other = emit_other
        self._emit_event = emit_event
        self._emit_probe = emit_probe
        # (kind, item, event_time, vehicle_id)
        self._pending: deque[tuple[str, Any, datetime | None, str | None]] = deque()
        self.held_back = 0
        self.probes_held_back = 0
        self.max_buffered = 0

    def other(self, item: Any) -> None:
        self._push((_OTHER, item, None, None))

    def event(self, item: Any, event_time: datetime, vehicle_id: str) -> None:
        self._push((_EVENT, item, event_time, vehicle_id))

    def probe(self, item: Any, event_time: datetime, vehicle_id: str | None) -> None:
        if self._emit_probe is None:
            raise ValueError("probe() needs emit_probe")
        self._push((_PROBE, item, event_time, vehicle_id))

    def _push(self, entry: tuple[str, Any, datetime | None, str | None]) -> None:
        self._pending.append(entry)
        self._drain(final=False)
        # Records still waiting after the drain (0 when nothing is pending).
        self.max_buffered = max(self.max_buffered, len(self._pending))

    def finish(self) -> None:
        self._drain(final=True)
        assert not self._pending

    def _later_events(self, limit: int) -> list[tuple[datetime, str]]:
        later: list[tuple[datetime, str]] = []
        for index, (kind, _item, event_time, vehicle_id) in enumerate(self._pending):
            if index == 0 or kind != _EVENT:
                continue
            later.append((event_time, vehicle_id))
            if len(later) >= limit:
                break
        return later

    def _held(self, event_time: datetime, vehicle_id: str | None, *, final: bool) -> bool | None:
        guard = self.guard
        if not guard.enabled or not guard.is_jump(event_time, self.tracker.max_event_time):
            return False
        later = self._later_events(guard.lookahead_events)
        complete = final or len(later) >= guard.lookahead_events
        return guard.decide(event_time, vehicle_id, later, complete=complete)

    def _drain(self, *, final: bool) -> None:
        while self._pending:
            kind, item, event_time, vehicle_id = self._pending[0]
            if kind == _OTHER:
                self._pending.popleft()
                self._emit_other(item)
                continue
            held = self._held(event_time, vehicle_id, final=final)
            if held is None:
                return  # wait for more of the stream
            self._pending.popleft()
            if kind == _PROBE:
                self.probes_held_back += held
                self._emit_probe(item, held)
                continue
            status, lateness, watermark = self.tracker.observe(event_time, advance=not held)
            self.held_back += held
            self._emit_event(item, EventTimeDecision(status, lateness, watermark, held))
