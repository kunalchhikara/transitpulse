"""Timetable construction (`schedules` / `schedule_stop_times`) and vehicle
assignment for one service day.

Timetable model:

- Each route runs `trips_per_route_per_day` departures from
  `service_start_time` every `headway_minutes`, alternating OUTBOUND (route's
  stop order) and INBOUND (reversed).
- Scheduled running time between consecutive stops is sampled **once per
  route segment** from `[min_segment_runtime_seconds,
  max_segment_runtime_seconds]` (a timetable is stable day to day), not
  derived from straight-line distance: DE-01 places stops uniformly across
  the Sydney bounding box, so straight-line segments are tens of km and a
  distance-derived timetable would yield multi-hour trips. See README
  "Known limitations".
- Vehicles are assigned per day from the route's pool (vehicles whose DE-01
  `assigned_route_id` is the route; falling back to the route's home-depot
  vehicles, then the whole fleet, if a pool is empty), picking the first
  vehicle free since its previous trip ended plus `min_layover_minutes`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from simulator.batch.config import BatchConfig
from simulator.batch.rng import derive_rng
from simulator.batch.schemas import Direction
from simulator.reference.generator import ReferenceDataset
from simulator.schemas.entities import Route, Vehicle


@dataclass(frozen=True)
class PlannedStop:
    stop_sequence: int
    stop_id: str
    arrival: datetime
    departure: datetime


@dataclass(frozen=True)
class PlannedTrip:
    trip_id: str
    route_id: str
    direction: Direction
    trip_sequence: int
    service_date: date
    vehicle_id: str
    depot_id: str
    stops: tuple[PlannedStop, ...]

    @property
    def scheduled_departure(self) -> datetime:
        return self.stops[0].departure

    @property
    def scheduled_arrival(self) -> datetime:
        return self.stops[-1].arrival


def local_to_utc(service_date: date, local_time: time, utc_offset_hours: int) -> datetime:
    """Convert a local wall-clock time on `service_date` to an aware UTC datetime."""
    naive_local = datetime.combine(service_date, local_time)
    return (naive_local - timedelta(hours=utc_offset_hours)).replace(tzinfo=UTC)


def local_midnight_utc(service_date: date, utc_offset_hours: int) -> datetime:
    return local_to_utc(service_date, time(0, 0), utc_offset_hours)


def compact_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def segment_runtimes(route: Route, config: BatchConfig) -> list[int]:
    """Scheduled seconds between consecutive stops of `route` (OUTBOUND order)."""
    rng = derive_rng(config.batch.seed, "segment-runtime", route.route_id)
    sched = config.schedules
    return [
        rng.randint(sched.min_segment_runtime_seconds, sched.max_segment_runtime_seconds)
        for _ in range(len(route.stop_ids) - 1)
    ]


def vehicle_pools(reference: ReferenceDataset) -> dict[str, list[Vehicle]]:
    """route_id -> ordered candidate vehicles (see module docstring)."""
    pools: dict[str, list[Vehicle]] = {}
    for route in reference.routes:
        pool = [v for v in reference.vehicles if v.assigned_route_id == route.route_id]
        if not pool:
            pool = [v for v in reference.vehicles if v.home_depot_id == route.home_depot_id]
        if not pool:
            pool = list(reference.vehicles)
        pools[route.route_id] = pool
    return pools


def _planned_stops(
    stop_ids: list[str], runtimes: list[int], departure: datetime, dwell_seconds: int
) -> tuple[PlannedStop, ...]:
    stops: list[PlannedStop] = []
    current = departure
    last = len(stop_ids) - 1
    for index, stop_id in enumerate(stop_ids):
        if index == 0:
            arrival = departure
            leave = departure
        else:
            arrival = current + timedelta(seconds=runtimes[index - 1])
            leave = arrival if index == last else arrival + timedelta(seconds=dwell_seconds)
        stops.append(
            PlannedStop(stop_sequence=index + 1, stop_id=stop_id, arrival=arrival, departure=leave)
        )
        current = leave
    return tuple(stops)


@dataclass(frozen=True)
class _Departure:
    route: Route
    trip_sequence: int
    direction: Direction
    stop_ids: list[str]
    runtimes: list[int]
    departure: datetime


def build_day_timetable(
    service_date: date,
    reference: ReferenceDataset,
    config: BatchConfig,
    runtimes_by_route: dict[str, list[int]],
    pools: dict[str, list[Vehicle]],
) -> tuple[list[PlannedTrip], int]:
    """Plan every trip for `service_date`, sorted by departure.

    Returns `(trips, overlap_count)` where `overlap_count` is how many trips
    had to be given a vehicle that was not yet free (only possible if a pool
    is too small for the configured headway/runtime; logged by the caller).
    """
    sched = config.schedules
    offset = config.batch.utc_offset_hours
    first_departure = local_to_utc(service_date, sched.service_start_time, offset)

    departures: list[_Departure] = []
    for route in reference.routes:
        outbound_runtimes = runtimes_by_route[route.route_id]
        for i in range(sched.trips_per_route_per_day):
            outbound = i % 2 == 0
            departures.append(
                _Departure(
                    route=route,
                    trip_sequence=i + 1,
                    direction=Direction.OUTBOUND if outbound else Direction.INBOUND,
                    stop_ids=list(route.stop_ids) if outbound else list(reversed(route.stop_ids)),
                    runtimes=(
                        list(outbound_runtimes) if outbound else list(reversed(outbound_runtimes))
                    ),
                    departure=first_departure + timedelta(minutes=i * sched.headway_minutes),
                )
            )
    departures.sort(key=lambda d: (d.departure, d.route.route_id, d.trip_sequence))

    layover = timedelta(minutes=sched.min_layover_minutes)
    available_at: dict[str, datetime] = {}
    overlap_count = 0
    trips: list[PlannedTrip] = []
    for dep in departures:
        stops = _planned_stops(dep.stop_ids, dep.runtimes, dep.departure, sched.dwell_seconds)
        pool = pools[dep.route.route_id]
        vehicle = next(
            (v for v in pool if available_at.get(v.vehicle_id, dep.departure) <= dep.departure),
            None,
        )
        if vehicle is None:
            overlap_count += 1
            vehicle = min(pool, key=lambda v: available_at[v.vehicle_id])
        available_at[vehicle.vehicle_id] = stops[-1].arrival + layover
        trips.append(
            PlannedTrip(
                trip_id=(
                    f"TRIP-{dep.route.route_id}-{compact_date(service_date)}-"
                    f"{dep.trip_sequence:03d}"
                ),
                route_id=dep.route.route_id,
                direction=dep.direction,
                trip_sequence=dep.trip_sequence,
                service_date=service_date,
                vehicle_id=vehicle.vehicle_id,
                depot_id=vehicle.home_depot_id,
                stops=stops,
            )
        )
    return trips, overlap_count
