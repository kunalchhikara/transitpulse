"""TelemetryEvent schema tests: all PROJECT.md section 6 fields present,
schema_version stamped, and a negative test proving a missing required
field fails validation (rather than silently defaulting)."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from simulator.config import load_validation_thresholds
from simulator.schemas.telemetry import (
    CURRENT_SCHEMA_VERSION,
    TelemetryEvent,
    VehicleStatus,
    validate_against_thresholds,
)

PROJECT_MD_SECTION_6_FIELDS = {
    "event_id",
    "schema_version",
    "generated_at",
    "event_time",
    "vehicle_id",
    "route_id",
    "trip_id",
    "latitude",
    "longitude",
    "speed_kmh",
    "heading",
    "occupancy",
    "delay_seconds",
    "status",
}


def _valid_payload() -> dict:
    now = datetime.now(UTC)
    return {
        "event_id": "EVT-00000001",
        "schema_version": CURRENT_SCHEMA_VERSION,
        "generated_at": now,
        "event_time": now,
        "vehicle_id": "VEH-0001",
        "route_id": "ROUTE-01",
        "trip_id": "TRIP-00000001",
        "latitude": -33.87,
        "longitude": 151.21,
        "speed_kmh": 42.5,
        "heading": 180.0,
        "occupancy": 12,
        "delay_seconds": 30,
        "status": VehicleStatus.IN_SERVICE,
    }


def test_schema_contains_all_project_md_section_6_fields():
    assert PROJECT_MD_SECTION_6_FIELDS.issubset(set(TelemetryEvent.model_fields.keys()))


def test_valid_event_constructs_and_round_trips():
    event = TelemetryEvent(**_valid_payload())
    assert event.schema_version == CURRENT_SCHEMA_VERSION
    dumped = event.model_dump(mode="json")
    for f in PROJECT_MD_SECTION_6_FIELDS:
        assert f in dumped


@pytest.mark.parametrize(
    "missing_field",
    sorted(PROJECT_MD_SECTION_6_FIELDS - {"schema_version"}),
)
def test_missing_required_field_fails_validation(missing_field):
    payload = _valid_payload()
    del payload[missing_field]
    with pytest.raises(ValidationError):
        TelemetryEvent(**payload)


def test_schema_version_defaults_when_omitted():
    payload = _valid_payload()
    del payload["schema_version"]
    event = TelemetryEvent(**payload)
    assert event.schema_version == CURRENT_SCHEMA_VERSION


def test_negative_speed_rejected_structurally():
    payload = _valid_payload()
    payload["speed_kmh"] = -5.0
    with pytest.raises(ValidationError):
        TelemetryEvent(**payload)


def test_invalid_latitude_rejected_structurally():
    payload = _valid_payload()
    payload["latitude"] = 999.0
    with pytest.raises(ValidationError):
        TelemetryEvent(**payload)


def test_validate_against_thresholds_flags_speed_over_configured_ceiling():
    thresholds = load_validation_thresholds()
    payload = _valid_payload()
    payload["speed_kmh"] = thresholds.max_speed_kmh + 50
    event = TelemetryEvent(**payload)
    reasons = validate_against_thresholds(
        event, thresholds, known_route_ids={"ROUTE-01"}
    )
    assert any("speed_kmh" in r for r in reasons)


def test_validate_against_thresholds_passes_clean_event():
    thresholds = load_validation_thresholds()
    event = TelemetryEvent(**_valid_payload())
    reasons = validate_against_thresholds(
        event, thresholds, known_route_ids={"ROUTE-01"}, now=event.event_time
    )
    assert reasons == []


def test_validate_against_thresholds_flags_unknown_route():
    thresholds = load_validation_thresholds()
    event = TelemetryEvent(**_valid_payload())
    reasons = validate_against_thresholds(
        event, thresholds, known_route_ids={"ROUTE-99"}, now=event.event_time
    )
    assert any("unknown route_id" in r for r in reasons)


def test_validate_against_thresholds_flags_far_future_event_time():
    thresholds = load_validation_thresholds()
    payload = _valid_payload()
    far_future = datetime.now(UTC).replace(year=datetime.now().year + 1)
    payload["event_time"] = far_future
    event = TelemetryEvent(**payload)
    reasons = validate_against_thresholds(event, thresholds, known_route_ids={"ROUTE-01"})
    assert any("future-time tolerance" in r for r in reasons)
