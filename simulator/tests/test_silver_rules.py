"""DE-06 rule catalogue: one positive and one negative test per PROJECT.md
section 12 rule (boundaries included), configurability through DE-01's
`ValidationThresholds`, rule precedence / all-failing-rules recording, and
the mapping of every DE-01 `validate_against_thresholds` reason."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from simulator.schemas.telemetry import TelemetryEvent, validate_against_thresholds
from simulator.silver import RULE_CATALOGUE, RULE_IDS, ValidationContractError
from simulator.silver.rules import (
    CATEGORY_PARSE,
    CATEGORY_RULE,
    CATEGORY_SCHEMA,
    THRESHOLD_REASON_RULES,
    threshold_reason_rule,
)
from simulator.tests._silver_helpers import (
    DELETE,
    PIPELINE_RULE_SETTINGS,
    PROCESSING_TIME,
    THRESHOLDS,
    valid_payload,
    validate,
    validator,
)

BASE = valid_payload()
GENERATED_AT = BASE["generated_at"]  # 2025-01-01T00:00:00Z


def _shift(seconds: float) -> str:
    base = TelemetryEvent.model_validate(BASE).generated_at
    return (base + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _rules(outcome) -> list[str]:
    return [v.rule for v in outcome.violations]


def test_baseline_de02_event_is_valid():
    outcome = validate(BASE)
    assert outcome.is_valid, outcome.violations
    assert outcome.event.event_id == BASE["event_id"]


# (rule, overrides) - each must be rejected with exactly that primary rule.
NEGATIVE = [
    # section 12: required event_id/vehicle_id
    ("required_fields", {"event_id": DELETE}),
    ("required_fields", {"event_id": None}),
    ("required_fields", {"event_id": "   "}),
    ("required_fields", {"vehicle_id": None}),  # DE-03 null_vehicle_id
    ("required_fields", {"vehicle_id": DELETE}),
    ("required_fields", {"occupancy": DELETE}),  # DE-03 missing_field
    # section 12: known route
    ("known_route", {"route_id": "ROUTE-UNKNOWN-9999"}),  # DE-03 unknown_route
    # section 12: valid latitude/longitude
    ("valid_coordinates", {"latitude": 999.0, "longitude": -999.0}),  # DE-03 invalid_coordinates
    ("valid_coordinates", {"latitude": 90.0001}),
    ("valid_coordinates", {"longitude": -180.5}),
    # section 12: nonnegative speed
    ("non_negative_speed", {"speed_kmh": -42.0}),  # DE-03 negative_speed
    ("non_negative_speed", {"speed_kmh": -0.01}),
    # section 12: configurable upper-speed threshold (default 120)
    ("max_speed", {"speed_kmh": 120.01}),
    # section 12: nonnegative occupancy (+ configured upper bound 150)
    ("non_negative_occupancy", {"occupancy": -1}),
    ("max_occupancy", {"occupancy": 151}),
    # section 12: parsable timestamps
    ("parsable_timestamps", {"event_time": "not-a-timestamp"}),  # DE-03 malformed_timestamp
    ("parsable_timestamps", {"generated_at": "2025-13-01T00:00:00Z"}),
    ("parsable_timestamps", {"event_time": "2025-01-01T00:00:00"}),  # no offset
    ("parsable_timestamps", {"event_time": 1735689600}),  # epoch number
    ("parsable_timestamps", {"event_time": "1735689600"}),  # epoch in a string
    # section 12: supported schema version
    ("supported_schema_version", {"schema_version": "2.0.0"}),
    ("supported_schema_version", {"schema_version": 1}),
    # section 12: future-time tolerance (default 120 s, vs generated_at)
    ("future_time_tolerance", {"event_time": _shift(120.001)}),
    # beyond section 12
    ("schema_conformance", {"status": "FLYING"}),
    ("schema_conformance", {"speed_kmh": "12.5"}),  # no string -> number coercion
    ("schema_conformance", {"occupancy": 5.5}),
    ("schema_conformance", {"unexpected_field": 1}),
    ("heading_range", {"heading": 360.0}),
    ("heading_range", {"heading": 359.95}),  # above configured 359.9
    # DE-08 (FU-08, FU-10, FU-06); boundaries and more cases in test_silver_value_bounds.py
    # and test_silver_watermark_guard.py
    ("service_area", {"latitude": 0.0, "longitude": 0.0}),
    ("delay_range", {"delay_seconds": 10**30}),
    ("delay_range", {"delay_seconds": -(10**30)}),
    ("timestamp_bounds", {"event_time": "9999-12-31T23:59:59Z", "generated_at": "9999-12-31T23:59:59Z"}),
    ("timestamp_bounds", {"event_time": "0001-01-01T00:00:00Z", "generated_at": "0001-01-01T00:00:00Z"}),
    ("watermark_guard", {"event_time": "2090-01-01T00:00:00Z", "generated_at": "2090-01-01T00:00:00Z"}),
]

#: Rules `run_silver` evaluates only because it passes the DE-01 service area
#: and the Silver watermark guard to the validator; their cases are validated
#: the same way (the other cases keep the plain validator, so e.g. the global
#: (90, 180) coordinate boundary is still tested on its own).
PIPELINE_ONLY_RULES = frozenset({"service_area", "watermark_guard"})


def _validate_for(rule, payload):
    return validate(payload, **PIPELINE_RULE_SETTINGS) if rule in PIPELINE_ONLY_RULES else validate(payload)


@pytest.mark.parametrize(("rule", "overrides"), NEGATIVE, ids=[f"{r}-{sorted(o)}" for r, o in NEGATIVE])
def test_negative_case_is_rejected_by_its_rule(rule, overrides):
    outcome = _validate_for(rule, valid_payload(**overrides))
    assert not outcome.is_valid
    assert outcome.event is None
    assert _rules(outcome)[0] == rule, outcome.violations


# Boundary / alternative values that must stay VALID.
POSITIVE = [
    ("required_fields", {"trip_id": "T"}),
    ("known_route", {"route_id": "ROUTE-10"}),
    ("valid_coordinates", {"latitude": 90.0, "longitude": 180.0}),
    ("valid_coordinates", {"latitude": -90.0, "longitude": -180.0}),
    ("non_negative_speed", {"speed_kmh": 0.0}),  # DE-03 breakdown speed
    ("non_negative_speed", {"speed_kmh": 0}),  # JSON int accepted for a float field
    ("max_speed", {"speed_kmh": 120.0}),
    ("non_negative_occupancy", {"occupancy": 0}),
    ("max_occupancy", {"occupancy": 150}),
    ("parsable_timestamps", {"event_time": "2025-01-01T11:00:00+11:00"}),  # Sydney DST offset
    ("parsable_timestamps", {"event_time": "2025-01-01T00:00:00.123456Z"}),
    ("supported_schema_version", {"schema_version": "1.0.0"}),
    ("future_time_tolerance", {"event_time": _shift(120)}),
    ("future_time_tolerance", {"event_time": _shift(-3600)}),  # late is not a validation failure
    ("heading_range", {"heading": 359.9}),
    ("heading_range", {"heading": 0.0}),
    ("service_area", {"latitude": -33.8688, "longitude": 151.2093}),  # Sydney CBD
    ("delay_range", {"delay_seconds": THRESHOLDS.max_delay_seconds}),
    ("delay_range", {"delay_seconds": THRESHOLDS.min_delay_seconds}),
    ("timestamp_bounds", {"event_time": "2000-01-01T00:00:00Z", "generated_at": "2000-01-01T00:00:00Z"}),
    # validate() ingests at PROCESSING_TIME (2025-02-01T10:00Z); guard skew 300 s.
    ("watermark_guard", {"event_time": "2025-02-01T10:05:00Z", "generated_at": "2025-02-01T10:05:00Z"}),
]


@pytest.mark.parametrize(("rule", "overrides"), POSITIVE, ids=[f"{r}-{sorted(o)}" for r, o in POSITIVE])
def test_positive_case_is_accepted(rule, overrides):
    outcome = _validate_for(rule, valid_payload(**overrides))
    assert outcome.is_valid, (rule, outcome.violations)


def test_every_section_12_rule_has_a_catalogue_entry_and_tests():
    section_12 = {
        "required event_id/vehicle_id", "known route where applicable", "valid latitude/longitude",
        "nonnegative speed", "configurable upper-speed threshold", "nonnegative occupancy",
        "parsable timestamps", "supported schema version", "future-time tolerance",
    }
    mapped = {r.project_rule for r in RULE_CATALOGUE if r.project_rule}
    assert section_12 <= {m.split(" (")[0] for m in mapped}
    tested_negative = {rule for rule, _ in NEGATIVE}
    tested_positive = {rule for rule, _ in POSITIVE}
    for spec in RULE_CATALOGUE:
        if spec.category == CATEGORY_PARSE:
            continue  # covered by test_silver_quarantine.py
        assert spec.rule_id in tested_negative, spec.rule_id
        if spec.rule_id != "schema_conformance":
            assert spec.rule_id in tested_positive, spec.rule_id
    assert {r.category for r in RULE_CATALOGUE} == {CATEGORY_PARSE, CATEGORY_SCHEMA, CATEGORY_RULE}
    assert list(RULE_IDS) == [r.rule_id for r in RULE_CATALOGUE]


# -- configurability (DE-01 ValidationThresholds) -----------------------------


def test_upper_speed_threshold_is_configurable():
    payload = valid_payload(speed_kmh=55.0)
    assert validate(payload).is_valid
    assert _rules(validate(payload, max_speed_kmh=50.0)) == ["max_speed"]


def test_minimum_speed_is_configurable_and_maps_to_non_negative_speed_rule():
    assert _rules(validate(valid_payload(speed_kmh=4.0), min_speed_kmh=5.0)) == ["non_negative_speed"]


def test_occupancy_bounds_are_configurable():
    payload = valid_payload(occupancy=100)
    assert validate(payload).is_valid
    assert _rules(validate(payload, max_occupancy=99)) == ["max_occupancy"]
    assert _rules(validate(valid_payload(occupancy=1), min_occupancy=2)) == ["non_negative_occupancy"]


def test_future_time_tolerance_is_configurable():
    payload = valid_payload(event_time=_shift(1))
    assert validate(payload).is_valid
    assert _rules(validate(payload, future_time_tolerance_seconds=0)) == ["future_time_tolerance"]


def test_supported_schema_versions_are_configurable():
    payload = valid_payload(schema_version="2.0.0")
    assert validate(payload, supported_schema_versions=["1.0.0", "2.0.0"]).is_valid


def test_known_route_rule_can_be_switched_off():
    payload = valid_payload(route_id="ROUTE-UNKNOWN-9999")
    assert validate(payload, require_known_route=False).is_valid


def test_required_fields_are_configurable_but_the_contract_still_applies():
    # Dropping trip_id from the configured list does not make a record without
    # it valid: TelemetryEvent itself requires it (reported under the same rule).
    required = [f for f in THRESHOLDS.required_fields if f != "trip_id"]
    outcome = validate(valid_payload(trip_id=DELETE), required_fields=required)
    assert _rules(outcome) == ["required_fields"]


# -- future-time reference -----------------------------------------------------


def test_future_time_is_measured_against_generated_at_not_the_wall_clock():
    # A whole stream simulated in 2030 is not "future" data: event_time ==
    # generated_at. Only an event claiming to be observed after it was sent is.
    payload = valid_payload(generated_at="2030-06-01T00:00:00Z", event_time="2030-06-01T00:00:00Z")
    assert validate(payload).is_valid


def test_ingestion_time_reference_is_available_for_live_feeds():
    payload = valid_payload(generated_at="2030-06-01T00:00:00Z", event_time="2030-06-01T00:00:00Z")
    v = validator()
    v.use_ingestion_time_reference = True
    outcome = v.validate(payload, json.dumps(payload), ingestion_time=PROCESSING_TIME)
    assert [x.rule for x in outcome.violations] == ["future_time_tolerance"]
    near = valid_payload(event_time=_shift(60), generated_at=_shift(60))
    within = v.validate(near, json.dumps(near), ingestion_time=TelemetryEvent.model_validate(BASE).generated_at)
    assert within.is_valid


# -- precedence and multiple failures -----------------------------------------


def test_all_failing_rules_of_the_failing_stage_are_recorded_in_catalogue_order():
    outcome = validate(valid_payload(latitude=999.0, speed_kmh=-1.0, vehicle_id=None, event_time="bad"))
    assert _rules(outcome) == ["required_fields", "parsable_timestamps", "valid_coordinates", "non_negative_speed"]
    assert [v.category for v in outcome.violations] == ["schema", "schema", "rule", "rule"]


def test_missing_fields_are_one_violation_listing_every_field():
    outcome = validate(valid_payload(vehicle_id=None, occupancy=DELETE))
    assert _rules(outcome) == ["required_fields"]
    assert "vehicle_id" in outcome.violations[0].reason and "occupancy" in outcome.violations[0].reason


def test_threshold_rules_are_evaluated_only_after_the_structural_stage_passes():
    # Documented: an unknown route is not reported alongside a structural failure.
    outcome = validate(valid_payload(route_id="ROUTE-UNKNOWN-9999", speed_kmh=-5.0))
    assert _rules(outcome) == ["non_negative_speed"]
    # Two threshold failures together are both reported.
    outcome = validate(valid_payload(route_id="ROUTE-UNKNOWN-9999", speed_kmh=500.0))
    assert _rules(outcome) == ["max_speed", "known_route"]


def test_unsupported_schema_version_short_circuits():
    outcome = validate(valid_payload(schema_version="9.9.9", latitude=999.0, vehicle_id=None))
    assert _rules(outcome) == ["supported_schema_version"]


# -- DE-01 reuse ---------------------------------------------------------------


def test_every_de01_threshold_reason_maps_to_a_rule():
    event = TelemetryEvent.model_validate(BASE)
    strict = THRESHOLDS.model_copy(
        update={
            "supported_schema_versions": ["0.0.0"], "max_speed_kmh": 1.0, "min_speed_kmh": 0.5,
            "max_occupancy": 1, "min_occupancy": 0, "min_heading_degrees": 300.0, "max_heading_degrees": 301.0,
            "future_time_tolerance_seconds": 0,
        }
    )
    fast = event.model_copy(update={"speed_kmh": 0.1, "occupancy": 3, "heading": 10.0,
                                    "event_time": event.event_time + timedelta(seconds=5)})
    reasons = validate_against_thresholds(fast, strict, known_route_ids=set(), now=event.generated_at)
    reasons += validate_against_thresholds(
        event.model_copy(update={"speed_kmh": 99.0, "heading": 350.0}), strict, known_route_ids=set(),
        now=event.generated_at,
    )
    reasons += validate_against_thresholds(
        event.model_copy(update={"occupancy": 1}),
        THRESHOLDS.model_copy(update={"min_occupancy": 2}), known_route_ids=None, now=event.generated_at,
    )
    # DE-08 reasons: service area, delay bounds, timestamp bounds.
    far = datetime(9999, 12, 31, tzinfo=UTC)
    reasons += validate_against_thresholds(
        event.model_copy(update={"latitude": 0.0, "longitude": 0.0, "delay_seconds": 10**30,
                                 "event_time": far, "generated_at": far}),
        THRESHOLDS, known_route_ids=None, now=far, service_area=PIPELINE_RULE_SETTINGS["service_area"],
    )
    reasons += validate_against_thresholds(
        event.model_copy(update={"delay_seconds": -(10**30)}), THRESHOLDS, known_route_ids=None,
        now=event.generated_at,
    )
    # Every DE-01 reason maps, and every mapping prefix is exercised by a real DE-01 reason.
    mapped = {threshold_reason_rule(r) for r in reasons}
    assert mapped == {rule for _, rule in THRESHOLD_REASON_RULES}
    for prefix, _ in THRESHOLD_REASON_RULES:
        assert any(r.startswith(prefix) for r in reasons), prefix


def test_unmapped_de01_reason_fails_loudly():
    with pytest.raises(ValidationContractError):
        threshold_reason_rule("a brand-new DE-01 reason")
