"""Schema-conformance test: in a no-incident run, 100% of emitted events
must validate against DE-01's `TelemetryEvent` contract, carry the current
schema_version, and never report an incident-only status (BREAKDOWN /
OUT_OF_SERVICE are DE-03's scope, not DE-02's)."""

from pydantic import ValidationError

from simulator.schemas.telemetry import (
    CURRENT_SCHEMA_VERSION,
    TelemetryEvent,
    VehicleStatus,
)
from simulator.tests._generator_helpers import build_generator

_EVENT_COUNT = 2000


def test_all_generated_events_validate_against_telemetry_event_schema():
    generator = build_generator()
    validated = 0
    for event in generator.events(_EVENT_COUNT):
        # Construction already validated it (TelemetryEvent is immutable
        # once built), but round-trip through raw JSON to independently
        # re-validate the wire representation too.
        reparsed = TelemetryEvent.model_validate_json(event.model_dump_json())
        assert reparsed == event
        validated += 1

    assert validated == _EVENT_COUNT


def test_all_generated_events_carry_current_schema_version():
    generator = build_generator()
    for event in generator.events(_EVENT_COUNT):
        assert event.schema_version == CURRENT_SCHEMA_VERSION


def test_no_incident_run_never_reports_incident_only_status():
    """DE-02 produces normal telemetry only; BREAKDOWN/OUT_OF_SERVICE are
    introduced by DE-03's incident scenarios, not this generator."""
    generator = build_generator()
    statuses = {event.status for event in generator.events(_EVENT_COUNT)}
    assert statuses.issubset({VehicleStatus.IN_SERVICE, VehicleStatus.DELAYED})


def test_events_have_unique_deterministic_event_ids():
    generator = build_generator()
    event_ids = [event.event_id for event in generator.events(_EVENT_COUNT)]
    assert len(event_ids) == len(set(event_ids)), "event_id must be unique per event"


def test_malformed_event_payload_still_fails_pydantic_validation():
    """Sanity check that the schema itself still rejects a bad payload --
    guards against a future change accidentally weakening TelemetryEvent."""
    generator = build_generator()
    good_payload = next(generator.events(1)).model_dump(mode="json")
    good_payload["latitude"] = 999.0
    try:
        TelemetryEvent(**good_payload)
    except ValidationError:
        pass
    else:
        raise AssertionError("expected invalid latitude to fail TelemetryEvent validation")
