"""`TelemetryGenerator`: deterministically produces normal (non-incident)
`TelemetryEvent`s for every vehicle in a DE-01 reference dataset.

Determinism contract: given the same `ScaleConfig` + `GeneratorConfig`
(hence the same seeds), `TelemetryGenerator(...).events()` yields a
byte-identical sequence of events across separate runs/processes, because:

1. The reference dataset (vehicles/routes/stops) is itself deterministic
   (DE-01's `generate_reference_dataset`).
2. Vehicles are visited round-robin in a fixed order (the reference
   dataset's list order) -- tick `n` always maps to the same vehicle.
3. All randomness is drawn from a single `random.Random(generator_config.
   seed)` instance, consumed in a fixed per-tick order (movement draw,
   then occupancy, then delay -- see `movement.advance_vehicle` and
   `produce_event` below).
4. `event_time`/`generated_at` come from a deterministic *virtual* timeline
   (`simulated_start_time + tick / events_per_second`), never from
   wall-clock `datetime.now()`.

No incident scenarios are modelled here (`status` is only ever IN_SERVICE
or DELAYED) -- see `docs/BACKLOG.md` DE-03 for BREAKDOWN/OUT_OF_SERVICE and
malformed/duplicate/late-event injection.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from random import Random

from simulator.config.models import ScaleConfig
from simulator.generator.config import GeneratorConfig
from simulator.generator.movement import (
    VehicleMovementState,
    advance_vehicle,
    initial_state,
)
from simulator.reference.generator import ReferenceDataset
from simulator.schemas.entities import Route, Stop, Vehicle
from simulator.schemas.telemetry import (
    CURRENT_SCHEMA_VERSION,
    TelemetryEvent,
    VehicleStatus,
)


class NoSimulableVehiclesError(RuntimeError):
    """Raised when the reference dataset has no vehicle whose
    `assigned_route_id` resolves to a known route -- there is nothing for
    the generator to simulate."""


@dataclass
class TelemetryGenerator:
    """Deterministic, stateful producer of one vehicle's `TelemetryEvent`
    per tick, round-robin across all simulable vehicles."""

    scale_config: ScaleConfig
    generator_config: GeneratorConfig
    reference: ReferenceDataset

    _rng: Random = field(init=False, repr=False)
    _routes_by_id: dict[str, Route] = field(init=False, repr=False)
    _stops_by_id: dict[str, Stop] = field(init=False, repr=False)
    _vehicles: list[Vehicle] = field(init=False, repr=False)
    _states: dict[str, VehicleMovementState] = field(init=False, repr=False)
    _event_seq: dict[str, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = Random(self.generator_config.seed)
        self._routes_by_id = {r.route_id: r for r in self.reference.routes}
        self._stops_by_id = {s.stop_id: s for s in self.reference.stops}
        self._vehicles = [
            v
            for v in self.reference.vehicles
            if v.assigned_route_id is not None and v.assigned_route_id in self._routes_by_id
        ]
        if not self._vehicles:
            raise NoSimulableVehiclesError(
                "no vehicle in the reference dataset has a resolvable assigned_route_id"
            )
        self._states = {
            v.vehicle_id: initial_state(self._routes_by_id[v.assigned_route_id])
            for v in self._vehicles
        }
        self._event_seq = {v.vehicle_id: 0 for v in self._vehicles}

    @property
    def vehicle_count(self) -> int:
        return len(self._vehicles)

    def _dt_seconds(self) -> float:
        """Simulated seconds between successive updates of the *same*
        vehicle: with round-robin scheduling across `vehicle_count`
        vehicles at an overall rate of `events_per_second`, one vehicle is
        revisited every `vehicle_count / events_per_second` seconds."""
        return self.vehicle_count / self.generator_config.events_per_second

    def produce_event(self, tick: int) -> TelemetryEvent:
        """Produce the single `TelemetryEvent` for global tick `tick`
        (0-indexed). Tick `n` always maps to vehicle `n % vehicle_count`,
        in the reference dataset's fixed list order."""
        vehicle = self._vehicles[tick % self.vehicle_count]
        route = self._routes_by_id[vehicle.assigned_route_id]
        state = self._states[vehicle.vehicle_id]

        new_state, sample = advance_vehicle(
            state=state,
            route=route,
            stops_by_id=self._stops_by_id,
            rng=self._rng,
            dt_seconds=self._dt_seconds(),
            config=self.generator_config,
        )
        self._states[vehicle.vehicle_id] = new_state

        event_time = self.generator_config.simulated_start_time + timedelta(
            seconds=tick / self.generator_config.events_per_second
        )

        seq = self._event_seq[vehicle.vehicle_id]
        self._event_seq[vehicle.vehicle_id] = seq + 1
        event_id = f"EVT-{vehicle.vehicle_id}-{seq:08d}"

        occupancy_fraction = self._rng.uniform(
            self.generator_config.min_occupancy_fraction,
            self.generator_config.max_occupancy_fraction,
        )
        occupancy = max(0, round(vehicle.capacity * occupancy_fraction))

        delay_seconds = self._rng.randint(
            self.generator_config.min_delay_seconds,
            self.generator_config.max_delay_seconds,
        )
        status = (
            VehicleStatus.DELAYED
            if delay_seconds >= self.generator_config.delayed_status_threshold_seconds
            else VehicleStatus.IN_SERVICE
        )

        trip_id = f"TRIP-{route.route_id}-{new_state.lap_number:04d}"

        return TelemetryEvent(
            event_id=event_id,
            schema_version=CURRENT_SCHEMA_VERSION,
            generated_at=event_time,
            event_time=event_time,
            vehicle_id=vehicle.vehicle_id,
            route_id=route.route_id,
            trip_id=trip_id,
            latitude=sample.latitude,
            longitude=sample.longitude,
            speed_kmh=sample.speed_kmh,
            heading=sample.heading,
            occupancy=occupancy,
            delay_seconds=delay_seconds,
            status=status,
        )

    def events(self, max_events: int | None = None) -> Iterator[TelemetryEvent]:
        """Yield `TelemetryEvent`s starting at tick 0. Unbounded unless
        `max_events` is given."""
        tick = 0
        while max_events is None or tick < max_events:
            yield self.produce_event(tick)
            tick += 1
