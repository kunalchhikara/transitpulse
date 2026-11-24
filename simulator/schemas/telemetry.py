"""The streaming TelemetryEvent contract: PROJECT.md section 6.

`CURRENT_SCHEMA_VERSION` is the version newly-generated events are stamped
with. `SUPPORTED_SCHEMA_VERSIONS` is the set a consumer currently accepts —
it can lag behind `CURRENT_SCHEMA_VERSION` while a downstream layer is
migrating, which is why `TelemetryEvent` itself does not restrict
`schema_version` to a closed enum: unsupported-version detection is a
*validation-threshold* concern (see `simulator/config/default_validation.toml`
-> `supported_schema_versions`, applied by a later Silver-layer validator),
not a structural-parsing concern.

Only `TelemetryEvent`'s structural field types/ranges are enforced here
(required-ness, type, physically-sane bounds). Business/operational
thresholds (max speed, future-time tolerance, known-route check, etc.) are
externalized to `simulator/config/` per `PROJECT.md` section 12 and section
19, and are applied by `validate_against_thresholds` below rather than
hardcoded on the model.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import Enum

from pydantic import BaseModel, Field

from simulator.config.models import BoundingBox, ValidationThresholds

#: The schema_version newly-generated TelemetryEvents are stamped with.
CURRENT_SCHEMA_VERSION = "1.0.0"

#: Schema versions this codebase can still structurally parse. Kept
#: separate from `ValidationThresholds.supported_schema_versions` (the
#: externally-configurable, business-facing "do we accept this version"
#: rule) — this constant is the hard structural floor.
SUPPORTED_SCHEMA_VERSIONS: tuple[str, ...] = (CURRENT_SCHEMA_VERSION,)


class VehicleStatus(str, Enum):
    """`status` field values a TelemetryEvent may carry."""

    IN_SERVICE = "IN_SERVICE"
    DELAYED = "DELAYED"
    BREAKDOWN = "BREAKDOWN"
    OUT_OF_SERVICE = "OUT_OF_SERVICE"


class TelemetryEvent(BaseModel):
    """One vehicle-telemetry event, per PROJECT.md section 6.

    Required fields: event_id, schema_version, generated_at, event_time,
    vehicle_id, route_id, trip_id, latitude, longitude, speed_kmh, heading,
    occupancy, delay_seconds, status. A missing required field fails
    validation at construction time (see
    `simulator/tests/test_telemetry_schema.py`).

    `generated_at` vs `event_time`: `event_time` is when the vehicle
    actually observed/measured the reading; `generated_at` is when the
    simulator produced the event record. They are expected to be equal (or
    very close) for normal events, and are deliberately allowed to diverge
    to simulate late/out-of-order delivery (PROJECT.md section 13,
    scenarios 3-4). Downstream ingestion additionally captures
    `ingestion_time`/`processing_time` (PROJECT.md section 6) — those are
    not part of this event contract; they belong to the Bronze layer that
    receives the event (a later story).
    """

    model_config = {"extra": "forbid"}

    event_id: str
    schema_version: str = CURRENT_SCHEMA_VERSION
    generated_at: datetime
    event_time: datetime
    vehicle_id: str
    route_id: str
    trip_id: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    speed_kmh: float = Field(ge=0.0)
    heading: float = Field(ge=0.0, lt=360.0)
    occupancy: int = Field(ge=0)
    delay_seconds: int
    status: VehicleStatus


def _as_utc_aware(value: datetime) -> datetime:
    """Naive values are taken as UTC (DE-01 convention, unchanged)."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def timestamp_bounds_reasons(thresholds: ValidationThresholds, **timestamps: datetime) -> list[str]:
    """DE-08 / FU-10: one reason per named timestamp outside the inclusive
    [`min_timestamp`, `max_timestamp`] bounds, in argument order. Shared by
    `validate_against_thresholds` and Gold's attribution re-check, so the rule
    has one implementation."""
    return [
        f"{name} outside configured timestamp bounds"
        for name, value in timestamps.items()
        if not thresholds.min_timestamp <= _as_utc_aware(value) <= thresholds.max_timestamp
    ]


def future_time_reason(event_time: datetime, thresholds: ValidationThresholds, now: datetime) -> str | None:
    """The future-time-tolerance reason if `event_time` is later than `now` +
    `future_time_tolerance_seconds`, else None. A reference so close to
    `datetime.max` that adding the tolerance overflows cannot be exceeded."""
    try:
        limit = _as_utc_aware(now) + timedelta(seconds=thresholds.future_time_tolerance_seconds)
    except OverflowError:
        return None
    if _as_utc_aware(event_time) > limit:
        return "event_time is beyond the future-time tolerance"
    return None


def in_service_area(latitude: float, longitude: float, area: BoundingBox, margin_degrees: float) -> bool:
    """DE-08 / FU-08: inside `area` expanded by `margin_degrees` on every side
    (edges inclusive)."""
    return (
        area.min_latitude - margin_degrees <= latitude <= area.max_latitude + margin_degrees
        and area.min_longitude - margin_degrees <= longitude <= area.max_longitude + margin_degrees
    )


def validate_against_thresholds(
    event: TelemetryEvent,
    thresholds: ValidationThresholds,
    *,
    known_route_ids: set[str] | None = None,
    now: datetime | None = None,
    service_area: BoundingBox | None = None,
) -> list[str]:
    """Apply the configurable PROJECT.md section 12 business rules.

    Returns a list of human-readable rejection reasons (empty if the event
    passes every configured threshold). This is intentionally separate from
    `TelemetryEvent` construction: a structurally-valid event can still be
    quarantined by a Silver-layer rule (e.g. speed over the configured
    ceiling, unknown route, unsupported schema version, timestamp too far in
    the future).

    DE-08 added (backward compatible - existing reasons and arguments are
    unchanged): `delay_seconds` bounds and absolute `event_time` /
    `generated_at` bounds (always applied), and the service-area check,
    applied only when the caller passes `service_area` (DE-01's
    `ScaleConfig.sydney_bounding_box`) and `thresholds.require_service_area`.
    """
    reasons: list[str] = []

    if event.schema_version not in thresholds.supported_schema_versions:
        reasons.append(f"unsupported schema_version '{event.schema_version}'")

    if (
        service_area is not None
        and thresholds.require_service_area
        and not in_service_area(event.latitude, event.longitude, service_area, thresholds.service_area_margin_degrees)
    ):
        reasons.append("position outside the configured service area")

    if event.speed_kmh < thresholds.min_speed_kmh:
        reasons.append("speed_kmh below configured minimum")
    if event.speed_kmh > thresholds.max_speed_kmh:
        reasons.append("speed_kmh exceeds configured maximum")

    if event.occupancy < thresholds.min_occupancy:
        reasons.append("occupancy below configured minimum")
    if event.occupancy > thresholds.max_occupancy:
        reasons.append("occupancy exceeds configured maximum")

    if event.heading < thresholds.min_heading_degrees:
        reasons.append("heading below configured minimum")
    if event.heading > thresholds.max_heading_degrees:
        reasons.append("heading exceeds configured maximum")

    if event.delay_seconds < thresholds.min_delay_seconds:
        reasons.append("delay_seconds below configured minimum")
    if event.delay_seconds > thresholds.max_delay_seconds:
        reasons.append("delay_seconds exceeds configured maximum")

    if (
        thresholds.require_known_route
        and known_route_ids is not None
        and event.route_id not in known_route_ids
    ):
        reasons.append(f"unknown route_id '{event.route_id}'")

    reasons.extend(
        timestamp_bounds_reasons(thresholds, event_time=event.event_time, generated_at=event.generated_at)
    )

    reference_now = now if now is not None else datetime.now(UTC)
    future = future_time_reason(event.event_time, thresholds, reference_now)
    if future is not None:
        reasons.append(future)

    return reasons
