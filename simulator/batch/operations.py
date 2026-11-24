"""Operated history for one service day: service disruptions, the operated
outcome of each scheduled trip, stop-arrival telemetry snapshots and
passenger journeys.

Per day, in a fixed order:

1. `plan_day_disruptions` draws `disruptions.per_day` disruptions from the
   day's RNG stream. DELAY disruptions target a route and a time window;
   BREAKDOWN/CANCEL disruptions target one scheduled trip (at most one such
   disruption per vehicle per day, so their effects never collide).
2. `execute_day` walks the day's trips in departure order. Each trip:
   - is CANCELLED if its vehicle is out of service (an earlier BREAKDOWN or
     CANCEL disruption on that vehicle whose window covers the departure),
     or if it is the target of a CANCEL disruption;
   - otherwise runs, drawing its start delay and per-segment delay drift from
     its own RNG stream and adding the impact of every DELAY disruption on
     its route whose window covers its departure. A BREAKDOWN target stops
     after a drawn stop (PARTIAL), which fixes the breakdown's start time and
     takes the vehicle out of service until the disruption ends.
   Passenger journeys and telemetry snapshots are derived from the operated
   stop times, so `occupancy`, `passenger_boardings` and `peak_occupancy` are
   all mutually consistent by construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from random import Random

from simulator.batch.config import (
    BatchConfig,
    DisruptionEffect,
    DisruptionTypeConfig,
    Severity,
)
from simulator.batch.rng import derive_rng
from simulator.batch.schemas import (
    DisruptionScope,
    DisruptionStatus,
    HistoricalTelemetrySnapshot,
    HistoricalTrip,
    PassengerJourney,
    ServiceDisruption,
    TripStatus,
)
from simulator.batch.timetable import PlannedTrip, compact_date, local_to_utc
from simulator.generator.movement import bearing_degrees
from simulator.reference.generator import ReferenceDataset
from simulator.schemas.telemetry import CURRENT_SCHEMA_VERSION, VehicleStatus

#: Source-system label stamped on every disruption log entry.
REPORTED_BY = "SYNTHETIC_OPS_CONTROL"


class InfeasibleDisruptionConfigError(ValueError):
    """Raised when more trip-scoped disruptions are requested for a day than
    there are distinct vehicles to target."""


@dataclass
class DisruptionPlan:
    disruption_id: str
    service_date: date
    type_config: DisruptionTypeConfig
    severity: Severity
    duration_minutes: int
    delay_impact_seconds: int
    route_id: str
    depot_id: str
    start: datetime | None = None
    end: datetime | None = None
    trip: PlannedTrip | None = None
    breakdown_stop_sequence: int | None = None
    stop_id: str | None = None
    affected_trip_ids: list[str] = field(default_factory=list)

    @property
    def effect(self) -> DisruptionEffect:
        return self.type_config.effect


@dataclass
class DayHistory:
    historical_trips: list[HistoricalTrip]
    historical_telemetry: list[HistoricalTelemetrySnapshot]
    passenger_journeys: list[PassengerJourney]
    service_disruptions: list[ServiceDisruption]


def _weighted_choice(rng: Random, items: list, weights: list[int]):
    return rng.choices(items, weights=weights, k=1)[0]


def plan_day_disruptions(
    service_date: date,
    trips: list[PlannedTrip],
    reference: ReferenceDataset,
    config: BatchConfig,
) -> list[DisruptionPlan]:
    cfg = config.disruptions
    rng = derive_rng(config.batch.seed, "disruptions", service_date.isoformat())
    severities = [s for s in Severity if s in cfg.severities]
    severity_weights = [cfg.severities[s].weight for s in severities]
    type_weights = [t.weight for t in cfg.types]
    sched = config.schedules
    window_start = local_to_utc(
        service_date, sched.service_start_time, config.batch.utc_offset_hours
    )
    window_minutes = int(
        (
            local_to_utc(service_date, sched.service_end_time, config.batch.utc_offset_hours)
            - window_start
        ).total_seconds()
        // 60
    )
    vehicles_by_id = {v.vehicle_id: v for v in reference.vehicles}
    targeted_vehicles: set[str] = set()

    plans: list[DisruptionPlan] = []
    for n in range(cfg.per_day):
        type_config = _weighted_choice(rng, cfg.types, type_weights)
        severity = _weighted_choice(rng, severities, severity_weights)
        severity_cfg = cfg.severities[severity]
        duration = rng.randint(type_config.min_duration_minutes, type_config.max_duration_minutes)
        impact = rng.randint(
            severity_cfg.min_delay_impact_seconds, severity_cfg.max_delay_impact_seconds
        )
        disruption_id = f"DIS-{compact_date(service_date)}-{n + 1:03d}"

        if type_config.effect == DisruptionEffect.DELAY:
            route = rng.choice(reference.routes)
            start = window_start + timedelta(minutes=rng.randint(0, window_minutes))
            plans.append(
                DisruptionPlan(
                    disruption_id=disruption_id,
                    service_date=service_date,
                    type_config=type_config,
                    severity=severity,
                    duration_minutes=duration,
                    delay_impact_seconds=impact,
                    route_id=route.route_id,
                    depot_id=route.home_depot_id,
                    start=start,
                    end=start + timedelta(minutes=duration),
                )
            )
            continue

        candidates = [t for t in trips if t.vehicle_id not in targeted_vehicles]
        if not candidates:
            raise InfeasibleDisruptionConfigError(
                f"{service_date}: no untargeted vehicle left for trip-scoped "
                f"disruption {disruption_id}; lower disruptions.per_day"
            )
        trip = rng.choice(candidates)
        targeted_vehicles.add(trip.vehicle_id)
        plan = DisruptionPlan(
            disruption_id=disruption_id,
            service_date=service_date,
            type_config=type_config,
            severity=severity,
            duration_minutes=duration,
            delay_impact_seconds=impact,
            route_id=trip.route_id,
            depot_id=vehicles_by_id[trip.vehicle_id].home_depot_id,
            trip=trip,
        )
        if type_config.effect == DisruptionEffect.BREAKDOWN:
            plan.breakdown_stop_sequence = rng.randint(1, len(trip.stops) - 1)
            plan.stop_id = trip.stops[plan.breakdown_stop_sequence - 1].stop_id
            # start/end fixed when the trip is executed (depends on its actual times).
        else:
            plan.start = trip.scheduled_departure
            plan.end = plan.start + timedelta(minutes=duration)
            plan.stop_id = trip.stops[0].stop_id
        plans.append(plan)
    return plans


def _heading(trip: PlannedTrip, index: int, stops_by_id: dict) -> float:
    """Bearing toward the next stop (from the previous stop at the terminus),
    rounded to 0.1 deg and kept within DE-01's [0, 360) bound."""
    if index + 1 < len(trip.stops):
        a, b = trip.stops[index], trip.stops[index + 1]
    else:
        a, b = trip.stops[index - 1], trip.stops[index]
    value = bearing_degrees(stops_by_id[a.stop_id], stops_by_id[b.stop_id])
    return round(value, 1) % 360.0


def _describe(plan: DisruptionPlan, route_number: str) -> str:
    label = plan.type_config.name.replace("_", " ").capitalize()
    if plan.effect == DisruptionEffect.DELAY:
        return (
            f"{label} on route {route_number} ({plan.route_id}): "
            f"estimated +{plan.delay_impact_seconds // 60} min delay to departing services."
        )
    assert plan.trip is not None
    if plan.effect == DisruptionEffect.BREAKDOWN:
        return (
            f"{label}: vehicle {plan.trip.vehicle_id} on route {route_number} "
            f"({plan.route_id}) failed at {plan.stop_id}; trip {plan.trip.trip_id} "
            f"terminated early and vehicle withdrawn."
        )
    return (
        f"{label}: trip {plan.trip.trip_id} on route {route_number} ({plan.route_id}) "
        f"cancelled; vehicle {plan.trip.vehicle_id} withdrawn."
    )


def execute_day(
    service_date: date,
    trips: list[PlannedTrip],
    plans: list[DisruptionPlan],
    reference: ReferenceDataset,
    config: BatchConfig,
) -> DayHistory:
    ops = config.operations
    pax = config.passengers
    seed = config.batch.seed
    stops_by_id = {s.stop_id: s for s in reference.stops}
    vehicles_by_id = {v.vehicle_id: v for v in reference.vehicles}
    routes_by_id = {r.route_id: r for r in reference.routes}
    fare_names = list(pax.fare_types)
    fare_weights = [pax.fare_types[name].weight for name in fare_names]

    trip_targets = {p.trip.trip_id: p for p in plans if p.trip is not None}
    delay_plans = [p for p in plans if p.effect == DisruptionEffect.DELAY]
    # vehicle_id -> list of trip-scoped plans whose out-of-service window is known.
    outages: dict[str, list[DisruptionPlan]] = {}
    for p in plans:
        if p.effect == DisruptionEffect.CANCEL:
            outages.setdefault(p.trip.vehicle_id, []).append(p)

    settle_trip = timedelta(minutes=ops.trip_record_settlement_minutes)
    settle_pax = timedelta(minutes=pax.settlement_lag_minutes)

    out_trips: list[HistoricalTrip] = []
    out_telemetry: list[HistoricalTelemetrySnapshot] = []
    out_journeys: list[PassengerJourney] = []

    for trip in trips:
        departure = trip.scheduled_departure
        own_plan = trip_targets.get(trip.trip_id)
        blocking = next(
            (
                p
                for p in outages.get(trip.vehicle_id, [])
                if p is not own_plan and p.start <= departure < p.end
            ),
            None,
        )
        if own_plan is not None and own_plan.effect == DisruptionEffect.CANCEL:
            blocking = own_plan

        common = {
            "event_date": service_date,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "vehicle_id": trip.vehicle_id,
            "depot_id": trip.depot_id,
            "direction": trip.direction,
            "scheduled_departure": departure,
            "scheduled_arrival": trip.scheduled_arrival,
            "stops_scheduled": len(trip.stops),
        }

        if blocking is not None:
            blocking.affected_trip_ids.append(trip.trip_id)
            out_trips.append(
                HistoricalTrip(
                    **common,
                    updated_at=trip.scheduled_arrival + settle_trip,
                    trip_status=TripStatus.CANCELLED,
                    actual_departure=None,
                    actual_arrival=None,
                    departure_delay_seconds=None,
                    arrival_delay_seconds=None,
                    stops_served=0,
                    passenger_boardings=0,
                    peak_occupancy=0,
                    disruption_delay_seconds=0,
                    disruption_id=blocking.disruption_id,
                )
            )
            continue

        rng = derive_rng(seed, "trip", trip.trip_id)
        active_delays = sorted(
            (
                p
                for p in delay_plans
                if p.route_id == trip.route_id and p.start <= departure < p.end
            ),
            key=lambda p: (p.start, p.disruption_id),
        )
        disruption_delay = sum(p.delay_impact_seconds for p in active_delays)
        for p in active_delays:
            p.affected_trip_ids.append(trip.trip_id)

        # Only a BREAKDOWN plan can still be attached here (CANCEL handled above).
        breakdown = own_plan
        served = breakdown.breakdown_stop_sequence if breakdown is not None else len(trip.stops)

        delays: list[int] = []
        delay = rng.randint(ops.min_start_delay_seconds, ops.max_start_delay_seconds)
        delay = max(ops.min_delay_seconds, delay + disruption_delay)
        for k in range(served):
            if k > 0:
                drift = rng.randint(ops.min_segment_drift_seconds, ops.max_segment_drift_seconds)
                delay = max(ops.min_delay_seconds, delay + drift)
            delays.append(delay)
        actual_arrivals = [
            trip.stops[k].arrival + timedelta(seconds=delays[k]) for k in range(served)
        ]
        actual_departures = [
            trip.stops[k].departure + timedelta(seconds=delays[k]) for k in range(served)
        ]

        # Passenger journeys (need >= 2 served stops to board and alight).
        vehicle = vehicles_by_id[trip.vehicle_id]
        journey_count = 0
        if served >= 2:
            journey_count = min(
                rng.randint(pax.min_journeys_per_trip, pax.max_journeys_per_trip),
                vehicle.capacity,
            )
        legs: list[tuple[int, int]] = []
        trip_suffix = trip.trip_id.removeprefix("TRIP-")
        for j in range(journey_count):
            board = rng.randint(1, served - 1)
            alight = rng.randint(board + 1, served)
            card = rng.randint(1, pax.card_population)
            fare_type = _weighted_choice(rng, fare_names, fare_weights)
            legs.append((board, alight))
            alighting_time = actual_arrivals[alight - 1]
            out_journeys.append(
                PassengerJourney(
                    event_date=service_date,
                    updated_at=alighting_time + settle_pax,
                    journey_id=f"JRN-{trip_suffix}-{j + 1:03d}",
                    card_id=f"CARD-{card:06d}",
                    trip_id=trip.trip_id,
                    route_id=trip.route_id,
                    vehicle_id=trip.vehicle_id,
                    boarding_stop_id=trip.stops[board - 1].stop_id,
                    boarding_stop_sequence=board,
                    alighting_stop_id=trip.stops[alight - 1].stop_id,
                    alighting_stop_sequence=alight,
                    boarding_time=actual_departures[board - 1],
                    alighting_time=alighting_time,
                    fare_type=fare_type,
                    fare_aud=round(pax.fare_types[fare_type].fare_aud, 2),
                )
            )

        # Stop-arrival telemetry snapshots. Occupancy on arrival at stop k
        # (before anyone alights) = journeys boarded before k, alighting at/after k.
        peak = 0
        for k in range(1, served + 1):
            occupancy = sum(1 for b, a in legs if b < k <= a)
            peak = max(peak, occupancy)
            is_breakdown_stop = breakdown is not None and k == served
            if is_breakdown_stop:
                status = VehicleStatus.BREAKDOWN
            elif delays[k - 1] >= ops.delayed_status_threshold_seconds:
                status = VehicleStatus.DELAYED
            else:
                status = VehicleStatus.IN_SERVICE
            stop = stops_by_id[trip.stops[k - 1].stop_id]
            event_time = actual_arrivals[k - 1]
            out_telemetry.append(
                HistoricalTelemetrySnapshot(
                    event_id=f"HEVT-{trip_suffix}-{k:03d}",
                    schema_version=CURRENT_SCHEMA_VERSION,
                    generated_at=event_time,
                    event_time=event_time,
                    vehicle_id=trip.vehicle_id,
                    route_id=trip.route_id,
                    trip_id=trip.trip_id,
                    latitude=stop.latitude,
                    longitude=stop.longitude,
                    speed_kmh=round(
                        rng.uniform(ops.min_arrival_speed_kmh, ops.max_arrival_speed_kmh), 2
                    ),
                    heading=_heading(trip, k - 1, stops_by_id),
                    occupancy=occupancy,
                    delay_seconds=delays[k - 1],
                    status=status,
                    stop_id=stop.stop_id,
                    stop_sequence=k,
                    event_date=service_date,
                    updated_at=event_time,
                )
            )

        completed = breakdown is None
        if breakdown is not None:
            breakdown.start = actual_arrivals[-1]
            breakdown.end = breakdown.start + timedelta(minutes=breakdown.duration_minutes)
            breakdown.affected_trip_ids.append(trip.trip_id)
            outages.setdefault(trip.vehicle_id, []).append(breakdown)
            primary = breakdown.disruption_id
        else:
            primary = active_delays[0].disruption_id if active_delays else None

        out_trips.append(
            HistoricalTrip(
                **common,
                updated_at=actual_arrivals[-1] + settle_trip,
                trip_status=TripStatus.COMPLETED if completed else TripStatus.PARTIAL,
                actual_departure=actual_departures[0],
                actual_arrival=actual_arrivals[-1] if completed else None,
                departure_delay_seconds=delays[0],
                arrival_delay_seconds=delays[-1] if completed else None,
                stops_served=served,
                passenger_boardings=journey_count,
                peak_occupancy=peak,
                disruption_delay_seconds=disruption_delay,
                disruption_id=primary,
            )
        )

    settle_disruption = timedelta(minutes=ops.disruption_record_settlement_minutes)
    out_disruptions: list[ServiceDisruption] = []
    for p in plans:
        is_trip_scoped = p.trip is not None
        route = routes_by_id[p.route_id]
        out_disruptions.append(
            ServiceDisruption(
                event_date=service_date,
                updated_at=p.end + settle_disruption,
                disruption_id=p.disruption_id,
                disruption_type=p.type_config.name,
                effect=p.effect,
                scope=DisruptionScope.TRIP if is_trip_scoped else DisruptionScope.ROUTE,
                severity=p.severity,
                status=DisruptionStatus.RESOLVED,
                route_id=p.route_id,
                depot_id=p.depot_id,
                vehicle_id=p.trip.vehicle_id if is_trip_scoped else None,
                trip_id=p.trip.trip_id if is_trip_scoped else None,
                stop_id=p.stop_id,
                start_time=p.start,
                end_time=p.end,
                duration_minutes=p.duration_minutes,
                estimated_delay_seconds=(
                    p.delay_impact_seconds if p.effect == DisruptionEffect.DELAY else None
                ),
                affected_trip_count=len(p.affected_trip_ids),
                description=_describe(p, route.route_number),
                reported_by=REPORTED_BY,
            )
        )

    return DayHistory(
        historical_trips=out_trips,
        historical_telemetry=out_telemetry,
        passenger_journeys=out_journeys,
        service_disruptions=out_disruptions,
    )
