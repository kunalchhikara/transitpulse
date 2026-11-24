"""`generate_batch_datasets`: deterministic orchestration of every DE-04
dataset for a `BatchConfig` + DE-01 `ScaleConfig`.

Determinism contract: same batch config (incl. `batch.seed`) + same scale
config (incl. its seed) -> byte-identical serialized output, in-process and
across separate OS processes. Guaranteed because the DE-01 reference dataset
is deterministic, all randomness comes from `rng.derive_rng` streams keyed on
the seed and stable identifiers, iteration order is fixed (service date ->
departure time -> route -> trip sequence), and nothing reads wall-clock time
or environment state. Output is also prefix-stable in `service_days` (see
`simulator/batch/rng.py`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import timedelta

from pydantic import BaseModel

from simulator.batch.config import BatchConfig
from simulator.batch.operations import execute_day, plan_day_disruptions
from simulator.batch.schemas import (
    DATASET_CATALOGUE,
    HistoricalTelemetrySnapshot,
    HistoricalTrip,
    PassengerJourney,
    ScheduledStopTime,
    ScheduledTrip,
    ServiceDisruption,
    WeatherObservation,
)
from simulator.batch.timetable import (
    build_day_timetable,
    local_midnight_utc,
    segment_runtimes,
    vehicle_pools,
)
from simulator.batch.weather import generate_day_weather
from simulator.config.models import ScaleConfig
from simulator.reference.generator import ReferenceDataset, generate_reference_dataset


@dataclass
class BatchDatasets:
    """All generated DE-04 rows, keyed by `DATASET_CATALOGUE` name."""

    schedules: list[ScheduledTrip] = field(default_factory=list)
    schedule_stop_times: list[ScheduledStopTime] = field(default_factory=list)
    historical_trips: list[HistoricalTrip] = field(default_factory=list)
    historical_telemetry: list[HistoricalTelemetrySnapshot] = field(default_factory=list)
    passenger_journeys: list[PassengerJourney] = field(default_factory=list)
    service_disruptions: list[ServiceDisruption] = field(default_factory=list)
    weather_observations: list[WeatherObservation] = field(default_factory=list)
    #: Trips that had to reuse a not-yet-free vehicle (pool too small). 0 at default scale.
    vehicle_overlap_count: int = 0

    def rows(self, name: str) -> list[BaseModel]:
        if name not in DATASET_CATALOGUE:
            raise KeyError(f"unknown dataset {name!r}")
        return getattr(self, name)

    def counts(self) -> dict[str, int]:
        return {name: len(self.rows(name)) for name in DATASET_CATALOGUE}


def serialize_row(row: BaseModel) -> str:
    """Canonical one-line JSON for a row: sorted keys, compact separators,
    UTC datetimes as ISO-8601 `Z`. This exact form is what is written to disk
    and compared byte-for-byte by the determinism tests."""
    return json.dumps(row.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def generate_batch_datasets(
    batch_config: BatchConfig,
    scale_config: ScaleConfig | None = None,
    *,
    reference: ReferenceDataset | None = None,
) -> BatchDatasets:
    """Generate every DE-04 dataset.

    Exactly one of `scale_config` / `reference` is normally given: the DE-01
    reference dataset is generated from `scale_config` unless a pre-built
    `reference` is supplied.
    """
    if reference is None:
        if scale_config is None:
            raise ValueError("either scale_config or reference must be provided")
        reference = generate_reference_dataset(scale_config)

    runtimes_by_route = {r.route_id: segment_runtimes(r, batch_config) for r in reference.routes}
    pools = vehicle_pools(reference)
    offset = batch_config.batch.utc_offset_hours
    publish_lead = timedelta(days=batch_config.schedules.publish_lead_days)

    out = BatchDatasets()
    for service_date in batch_config.batch.service_dates:
        trips, overlaps = build_day_timetable(
            service_date, reference, batch_config, runtimes_by_route, pools
        )
        out.vehicle_overlap_count += overlaps
        published_at = local_midnight_utc(service_date, offset) - publish_lead
        for trip in trips:
            out.schedules.append(
                ScheduledTrip(
                    event_date=service_date,
                    updated_at=published_at,
                    trip_id=trip.trip_id,
                    route_id=trip.route_id,
                    direction=trip.direction,
                    trip_sequence=trip.trip_sequence,
                    vehicle_id=trip.vehicle_id,
                    depot_id=trip.depot_id,
                    origin_stop_id=trip.stops[0].stop_id,
                    destination_stop_id=trip.stops[-1].stop_id,
                    stop_count=len(trip.stops),
                    scheduled_departure=trip.scheduled_departure,
                    scheduled_arrival=trip.scheduled_arrival,
                )
            )
            for stop in trip.stops:
                out.schedule_stop_times.append(
                    ScheduledStopTime(
                        event_date=service_date,
                        updated_at=published_at,
                        trip_id=trip.trip_id,
                        stop_sequence=stop.stop_sequence,
                        stop_id=stop.stop_id,
                        scheduled_arrival=stop.arrival,
                        scheduled_departure=stop.departure,
                    )
                )

        plans = plan_day_disruptions(service_date, trips, reference, batch_config)
        history = execute_day(service_date, trips, plans, reference, batch_config)
        out.historical_trips.extend(history.historical_trips)
        out.historical_telemetry.extend(history.historical_telemetry)
        out.passenger_journeys.extend(history.passenger_journeys)
        out.service_disruptions.extend(history.service_disruptions)
        out.weather_observations.extend(generate_day_weather(service_date, reference, batch_config))
    return out
