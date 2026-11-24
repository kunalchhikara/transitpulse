"""Row schemas and the dataset catalogue for DE-04's batch sources.

Every dataset row model:

- carries `schema_version` (`BATCH_SCHEMA_VERSION`, except
  `HistoricalTelemetrySnapshot`, which extends DE-01's streaming
  `TelemetryEvent` contract and therefore shares its `CURRENT_SCHEMA_VERSION`);
- carries `updated_at` (UTC) - the **watermark column** a later incremental
  load (PROJECT.md section 11, DE-05+) filters on (`updated_at > last_watermark`);
- carries `event_date` - the **partition column** (local operating/service
  date for trip-derived datasets; local observation date for weather);
- forbids unknown fields (`extra="forbid"`), so schema drift fails loudly.

`DATASET_CATALOGUE` is the single machine-readable description of each
dataset (file name, model, primary key, watermark/partition columns, foreign
keys, downstream Gold/Warehouse targets). The writer, the manifest, the
tests and the README catalogue are all driven from / checked against it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from simulator.batch.config import DisruptionEffect, Severity
from simulator.schemas.telemetry import TelemetryEvent

#: schema_version stamped on every DE-04 batch row (except historical
#: telemetry, see module docstring). Bump on any breaking field change.
BATCH_SCHEMA_VERSION = "1.0.0"


class Direction(str, Enum):
    OUTBOUND = "OUTBOUND"
    INBOUND = "INBOUND"


class TripStatus(str, Enum):
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"  # vehicle broke down mid-trip
    CANCELLED = "CANCELLED"


class DisruptionScope(str, Enum):
    ROUTE = "ROUTE"
    TRIP = "TRIP"


class DisruptionStatus(str, Enum):
    RESOLVED = "RESOLVED"


class WeatherCondition(str, Enum):
    CLEAR = "CLEAR"
    CLOUDY = "CLOUDY"
    RAIN = "RAIN"
    HEAVY_RAIN = "HEAVY_RAIN"
    STORM = "STORM"


class _BatchRow(BaseModel):
    model_config = {"extra": "forbid"}

    schema_version: str = BATCH_SCHEMA_VERSION
    event_date: date
    updated_at: AwareDatetime


class ScheduledTrip(_BatchRow):
    """`schedules`: one row per timetabled trip (trip-level header)."""

    trip_id: str
    route_id: str
    direction: Direction
    trip_sequence: int = Field(ge=1, description="1-based order of the trip on its route that day.")
    vehicle_id: str
    depot_id: str = Field(description="Operating depot = assigned vehicle's home depot.")
    origin_stop_id: str
    destination_stop_id: str
    stop_count: int = Field(ge=2)
    scheduled_departure: AwareDatetime
    scheduled_arrival: AwareDatetime

    @model_validator(mode="after")
    def _check(self) -> ScheduledTrip:
        if self.scheduled_arrival <= self.scheduled_departure:
            raise ValueError("scheduled_arrival must be after scheduled_departure")
        return self


class ScheduledStopTime(_BatchRow):
    """`schedule_stop_times`: one row per (trip, stop) timetable entry."""

    trip_id: str
    stop_sequence: int = Field(ge=1)
    stop_id: str
    scheduled_arrival: AwareDatetime
    scheduled_departure: AwareDatetime

    @model_validator(mode="after")
    def _check(self) -> ScheduledStopTime:
        if self.scheduled_departure < self.scheduled_arrival:
            raise ValueError("scheduled_departure must be >= scheduled_arrival")
        return self


class HistoricalTrip(_BatchRow):
    """`historical_trips`: the operated outcome of each scheduled trip."""

    trip_id: str
    route_id: str
    vehicle_id: str
    depot_id: str
    direction: Direction
    trip_status: TripStatus
    scheduled_departure: AwareDatetime
    scheduled_arrival: AwareDatetime
    actual_departure: AwareDatetime | None
    actual_arrival: AwareDatetime | None
    departure_delay_seconds: int | None
    arrival_delay_seconds: int | None
    stops_scheduled: int = Field(ge=2)
    stops_served: int = Field(ge=0)
    passenger_boardings: int = Field(ge=0)
    peak_occupancy: int = Field(ge=0)
    disruption_delay_seconds: int = Field(ge=0)
    disruption_id: str | None

    @model_validator(mode="after")
    def _check(self) -> HistoricalTrip:
        if self.stops_served > self.stops_scheduled:
            raise ValueError("stops_served cannot exceed stops_scheduled")
        completed = self.trip_status == TripStatus.COMPLETED
        if completed and (
            self.actual_arrival is None or self.stops_served != self.stops_scheduled
        ):
            raise ValueError("COMPLETED trips need actual_arrival and all stops served")
        cancelled = self.trip_status == TripStatus.CANCELLED
        if cancelled and (self.actual_departure is not None or self.stops_served != 0):
            raise ValueError("CANCELLED trips have no actuals")
        if self.trip_status != TripStatus.COMPLETED and self.disruption_id is None:
            raise ValueError("non-COMPLETED trips must reference a disruption_id")
        return self


class HistoricalTelemetrySnapshot(TelemetryEvent):
    """`historical_telemetry`: a stop-arrival AVL snapshot.

    A strict superset of DE-01's streaming `TelemetryEvent` (same field
    names/types/bounds, same `schema_version`), so Silver-layer validation
    written for the stream also applies to history. The extra fields locate
    the snapshot on the timetable and provide watermark/partition columns.
    """

    stop_id: str
    stop_sequence: int = Field(ge=1)
    snapshot_type: str = "STOP_ARRIVAL"
    event_date: date
    updated_at: AwareDatetime


class PassengerJourney(_BatchRow):
    """`passenger_journeys`: one tap-on/tap-off journey on one trip."""

    journey_id: str
    card_id: str = Field(description="Synthetic pseudonymous card token - not PII.")
    trip_id: str
    route_id: str
    vehicle_id: str
    boarding_stop_id: str
    boarding_stop_sequence: int = Field(ge=1)
    alighting_stop_id: str
    alighting_stop_sequence: int = Field(ge=2)
    boarding_time: AwareDatetime
    alighting_time: AwareDatetime
    fare_type: str
    fare_aud: float = Field(ge=0.0)

    @model_validator(mode="after")
    def _check(self) -> PassengerJourney:
        if self.alighting_stop_sequence <= self.boarding_stop_sequence:
            raise ValueError("alighting_stop_sequence must be after boarding_stop_sequence")
        if self.alighting_time <= self.boarding_time:
            raise ValueError("alighting_time must be after boarding_time")
        return self


class ServiceDisruption(_BatchRow):
    """`service_disruptions`: one operational incident log entry.

    Field set chosen to populate a later `fact_service_incident`
    (PROJECT.md section 10): natural key, date/time keys (event_date,
    start_time), dimension keys (route/vehicle/stop/depot), degenerate
    attributes (type/effect/scope/severity/status/description) and
    measures (duration_minutes, estimated_delay_seconds, affected_trip_count).
    """

    disruption_id: str
    disruption_type: str
    effect: DisruptionEffect
    scope: DisruptionScope
    severity: Severity
    status: DisruptionStatus
    route_id: str
    depot_id: str
    vehicle_id: str | None
    trip_id: str | None
    stop_id: str | None
    start_time: AwareDatetime
    end_time: AwareDatetime
    duration_minutes: int = Field(gt=0)
    estimated_delay_seconds: int | None
    affected_trip_count: int = Field(ge=0)
    description: str
    reported_by: str

    @model_validator(mode="after")
    def _check(self) -> ServiceDisruption:
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.scope == DisruptionScope.TRIP and (self.vehicle_id is None or self.trip_id is None):
            raise ValueError("TRIP-scoped disruptions need vehicle_id and trip_id")
        return self


class WeatherObservation(_BatchRow):
    """`weather_observations` (optional): synthetic depot weather station readings."""

    observation_id: str
    station_id: str
    depot_id: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    observed_at: AwareDatetime
    temperature_c: float
    rainfall_mm: float = Field(ge=0.0)
    wind_speed_kmh: float = Field(ge=0.0)
    condition: WeatherCondition


@dataclass(frozen=True)
class ForeignKey:
    column: str
    references: str  # "<dataset>.<column>"; reference datasets: depots/stops/routes/vehicles
    nullable: bool = False


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    model: type[BaseModel]
    primary_key: tuple[str, ...]
    watermark_column: str
    partition_column: str
    foreign_keys: tuple[ForeignKey, ...]
    supports: tuple[str, ...]

    @property
    def file_name(self) -> str:
        return f"{self.name}.jsonl"


_COMMON = {"watermark_column": "updated_at", "partition_column": "event_date"}

#: Ordered catalogue of every DE-04 dataset. Order = write order.
DATASET_CATALOGUE: dict[str, DatasetSpec] = {
    spec.name: spec
    for spec in (
        DatasetSpec(
            name="schedules",
            model=ScheduledTrip,
            primary_key=("trip_id",),
            foreign_keys=(
                ForeignKey("route_id", "routes.route_id"),
                ForeignKey("vehicle_id", "vehicles.vehicle_id"),
                ForeignKey("depot_id", "depots.depot_id"),
                ForeignKey("origin_stop_id", "stops.stop_id"),
                ForeignKey("destination_stop_id", "stops.stop_id"),
            ),
            supports=("fact_trip (planned measures)", "dim_route", "dim_date", "dim_time"),
            **_COMMON,
        ),
        DatasetSpec(
            name="schedule_stop_times",
            model=ScheduledStopTime,
            primary_key=("trip_id", "stop_sequence"),
            foreign_keys=(
                ForeignKey("trip_id", "schedules.trip_id"),
                ForeignKey("stop_id", "stops.stop_id"),
            ),
            supports=("fact_route_performance (stop-level punctuality)", "dim_stop"),
            **_COMMON,
        ),
        DatasetSpec(
            name="historical_trips",
            model=HistoricalTrip,
            primary_key=("trip_id",),
            foreign_keys=(
                ForeignKey("trip_id", "schedules.trip_id"),
                ForeignKey("route_id", "routes.route_id"),
                ForeignKey("vehicle_id", "vehicles.vehicle_id"),
                ForeignKey("depot_id", "depots.depot_id"),
                ForeignKey("disruption_id", "service_disruptions.disruption_id", nullable=True),
            ),
            supports=("fact_trip", "fact_route_performance", "dim_vehicle", "dim_depot"),
            **_COMMON,
        ),
        DatasetSpec(
            name="historical_telemetry",
            model=HistoricalTelemetrySnapshot,
            primary_key=("event_id",),
            foreign_keys=(
                ForeignKey("trip_id", "schedules.trip_id"),
                ForeignKey("route_id", "routes.route_id"),
                ForeignKey("vehicle_id", "vehicles.vehicle_id"),
                ForeignKey("stop_id", "stops.stop_id"),
            ),
            supports=("fact_vehicle_telemetry", "fact_route_performance"),
            **_COMMON,
        ),
        DatasetSpec(
            name="passenger_journeys",
            model=PassengerJourney,
            primary_key=("journey_id",),
            foreign_keys=(
                ForeignKey("trip_id", "schedules.trip_id"),
                ForeignKey("route_id", "routes.route_id"),
                ForeignKey("vehicle_id", "vehicles.vehicle_id"),
                ForeignKey("boarding_stop_id", "stops.stop_id"),
                ForeignKey("alighting_stop_id", "stops.stop_id"),
            ),
            supports=("fact_passenger_journey", "dim_stop", "dim_date", "dim_time"),
            **_COMMON,
        ),
        DatasetSpec(
            name="service_disruptions",
            model=ServiceDisruption,
            primary_key=("disruption_id",),
            foreign_keys=(
                ForeignKey("route_id", "routes.route_id"),
                ForeignKey("depot_id", "depots.depot_id"),
                ForeignKey("vehicle_id", "vehicles.vehicle_id", nullable=True),
                ForeignKey("trip_id", "schedules.trip_id", nullable=True),
                ForeignKey("stop_id", "stops.stop_id", nullable=True),
            ),
            supports=("fact_service_incident", "fact_route_performance (reliability)"),
            **_COMMON,
        ),
        DatasetSpec(
            name="weather_observations",
            model=WeatherObservation,
            primary_key=("observation_id",),
            foreign_keys=(ForeignKey("depot_id", "depots.depot_id"),),
            supports=("fact_route_performance (weather context, optional)", "dim_date", "dim_time"),
            **_COMMON,
        ),
    )
}
