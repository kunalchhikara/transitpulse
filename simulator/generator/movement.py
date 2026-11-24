"""The route/stop-sequence movement model.

A vehicle alternates between two states along its assigned `Route`'s
ordered `stop_ids`:

- **at_stop**: stationary at its current stop, reporting near-zero speed,
  until (probabilistically, per tick) it departs.
- **cruising**: moving in a straight line from its current stop toward the
  next one at a sampled cruising speed; position is linearly interpolated
  by the fraction of the segment covered in `dt_seconds` of *simulated*
  time. On reaching the next stop the vehicle snaps exactly onto that
  stop's coordinates and becomes `at_stop` again; after the last stop it
  loops back to the first (`lap_number` increments), modelling an ongoing
  route service.

Because position only ever changes by `speed_kmh * dt_seconds` of travel
per tick (bounded by the configured cruise-speed range), consecutive events
for the same vehicle can never "teleport" an implausible distance -- see
`simulator/tests/test_movement.py`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from random import Random

from simulator.generator.config import GeneratorConfig
from simulator.schemas.entities import Route, Stop

_EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class VehicleMovementState:
    """Where a vehicle currently is along its route."""

    current_stop_index: int
    next_stop_index: int
    fraction: float  # progress along the current_stop -> next_stop segment, [0, 1)
    at_stop: bool
    lap_number: int


@dataclass(frozen=True)
class VehicleTelemetrySample:
    """The physically-derived fields `advance_vehicle` produces for one tick."""

    latitude: float
    longitude: float
    speed_kmh: float
    heading: float


def initial_state(route: Route) -> VehicleMovementState:
    """A vehicle starts dwelling at the first stop on its route."""
    num_stops = len(route.stop_ids)
    return VehicleMovementState(
        current_stop_index=0,
        next_stop_index=1 % num_stops,
        fraction=0.0,
        at_stop=True,
        lap_number=0,
    )


def haversine_km(a: Stop, b: Stop) -> float:
    """Great-circle distance between two stops, in kilometres."""
    lat1, lon1, lat2, lon2 = map(
        math.radians, (a.latitude, a.longitude, b.latitude, b.longitude)
    )
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(h)))


def bearing_degrees(a: Stop, b: Stop) -> float:
    """Initial compass bearing travelling from stop `a` toward stop `b`,
    in degrees, normalized to `[0, 360)` (matching `TelemetryEvent.heading`'s
    structural bound)."""
    lat1, lon1, lat2, lon2 = map(
        math.radians, (a.latitude, a.longitude, b.latitude, b.longitude)
    )
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(
        dlon
    )
    return math.degrees(math.atan2(x, y)) % 360.0


def _interpolate(a: Stop, b: Stop, fraction: float) -> tuple[float, float]:
    latitude = a.latitude + (b.latitude - a.latitude) * fraction
    longitude = a.longitude + (b.longitude - a.longitude) * fraction
    return latitude, longitude


def _round_heading(value: float) -> float:
    # `% 360.0` after rounding guards against a value that rounds up to
    # exactly 360.0, which would violate TelemetryEvent.heading's `lt=360`
    # structural bound.
    return round(value, 2) % 360.0


def advance_vehicle(
    *,
    state: VehicleMovementState,
    route: Route,
    stops_by_id: dict[str, Stop],
    rng: Random,
    dt_seconds: float,
    config: GeneratorConfig,
) -> tuple[VehicleMovementState, VehicleTelemetrySample]:
    """Advance one vehicle by one tick (`dt_seconds` of simulated time).

    Draws from `rng` in a fixed order (depart-decision/speed first, no
    other draws) so that, given the same seed and config, the sequence of
    states/samples produced across repeated calls is deterministic.
    """
    stop_ids = route.stop_ids
    num_stops = len(stop_ids)
    current_stop = stops_by_id[stop_ids[state.current_stop_index]]
    next_stop = stops_by_id[stop_ids[state.next_stop_index]]
    heading = _round_heading(bearing_degrees(current_stop, next_stop))

    if state.at_stop:
        departs = rng.random() < config.depart_probability_per_tick
        speed_kmh = (
            rng.uniform(config.cruise_speed_min_kmh, config.cruise_speed_max_kmh)
            if departs
            else rng.uniform(0.0, config.dwell_speed_jitter_kmh)
        )
        new_state = (
            VehicleMovementState(
                current_stop_index=state.current_stop_index,
                next_stop_index=state.next_stop_index,
                fraction=0.0,
                at_stop=False,
                lap_number=state.lap_number,
            )
            if departs
            else state
        )
        sample = VehicleTelemetrySample(
            latitude=current_stop.latitude,
            longitude=current_stop.longitude,
            speed_kmh=round(speed_kmh, 2),
            heading=heading,
        )
        return new_state, sample

    speed_kmh = rng.uniform(config.cruise_speed_min_kmh, config.cruise_speed_max_kmh)
    segment_km = haversine_km(current_stop, next_stop)
    distance_km = speed_kmh * (dt_seconds / 3600.0)
    fraction_step = distance_km / segment_km if segment_km > 1e-9 else 1.0
    new_fraction = state.fraction + fraction_step

    if new_fraction >= 1.0:
        arrived_index = state.next_stop_index
        upcoming_index = (arrived_index + 1) % num_stops
        lap_number = state.lap_number + (1 if arrived_index == 0 else 0)
        arrival_stop = stops_by_id[stop_ids[arrived_index]]
        new_state = VehicleMovementState(
            current_stop_index=arrived_index,
            next_stop_index=upcoming_index,
            fraction=0.0,
            at_stop=True,
            lap_number=lap_number,
        )
        sample = VehicleTelemetrySample(
            latitude=arrival_stop.latitude,
            longitude=arrival_stop.longitude,
            speed_kmh=round(speed_kmh, 2),
            heading=heading,
        )
        return new_state, sample

    new_state = VehicleMovementState(
        current_stop_index=state.current_stop_index,
        next_stop_index=state.next_stop_index,
        fraction=new_fraction,
        at_stop=False,
        lap_number=state.lap_number,
    )
    latitude, longitude = _interpolate(current_stop, next_stop, new_fraction)
    sample = VehicleTelemetrySample(
        latitude=latitude,
        longitude=longitude,
        speed_kmh=round(speed_kmh, 2),
        heading=heading,
    )
    return new_state, sample
