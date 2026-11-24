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

from simulator.config.models import ValidationThresholds

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


def validate_against_thresholds(
    event: TelemetryEvent,
    thresholds: ValidationThresholds,
    *,
    known_route_ids: set[str] | None = None,
    now: datetime | None = None,
) -> list[str]:
    """Apply the configurable PROJECT.md section 12 business rules.

    Returns a list of human-readable rejection reasons (empty if the event
    passes every configured threshold). This is intentionally separate from
    `TelemetryEvent` construction: a structurally-valid event can still be
    quarantined by a Silver-layer rule (e.g. speed over the configured
    ceiling, unknown route, unsupported schema version, timestamp too far in
    the future).
    """
    reasons: list[str] = []

    if event.schema_version not in thresholds.supported_schema_versions:
        reasons.append(f"unsupported schema_version '{event.schema_version}'")

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

    if (
        thresholds.require_known_route
        and known_route_ids is not None
        and event.route_id not in known_route_ids
    ):
        reasons.append(f"unknown route_id '{event.route_id}'")

    reference_now = now if now is not None else datetime.now(UTC)
    tolerance = timedelta(seconds=thresholds.future_time_tolerance_seconds)
    event_time = event.event_time
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    if event_time > reference_now + tolerance:
        reasons.append("event_time is beyond the future-time tolerance")

    return reasons
