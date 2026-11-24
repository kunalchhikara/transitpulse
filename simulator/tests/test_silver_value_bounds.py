"""DE-08 Silver value bounds (BACKLOG FU-08, FU-10): positive, negative and
boundary cases for each new rule, configurability through DE-01
`ValidationThresholds`, rule precedence, and the same cases end to end
through DE-05 Bronze -> DE-06 Silver (the run commits, the record is
quarantined losslessly, and no clean record carries an out-of-bounds value).

New rules (README "DE-06: Rule catalogue"):
* `delay_range` - `min_delay_seconds <= delay_seconds <= max_delay_seconds`
* `service_area` - position inside DE-01 `sydney_bounding_box` expanded by
  `service_area_margin_degrees` (when `require_service_area`)
* `timestamp_bounds` - `min_timestamp <= event_time, generated_at <= max_timestamp`
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from simulator.config import ValidationThresholds, load_scale_config
from simulator.reference.generator import generate_reference_dataset
from simulator.silver.rules import TelemetryValidator
from simulator.tests._silver_helpers import (
    PROCESSING_TIME,
    THRESHOLDS,
    lines_to_silver,
    table,
    valid_payload,
)

SCALE = load_scale_config()
BOX = SCALE.sydney_bounding_box
ROUTES = [r.route_id for r in generate_reference_dataset(SCALE).routes]
BASE = valid_payload()


def _validator(**threshold_overrides) -> TelemetryValidator:
    """Constructed the way `run_silver` constructs it (service area = DE-01 box)."""
    thresholds = THRESHOLDS.model_copy(update=threshold_overrides) if threshold_overrides else THRESHOLDS
    return TelemetryValidator(thresholds, ROUTES, service_area=BOX)


def _rules(payload, **threshold_overrides) -> list[str]:
    outcome = _validator(**threshold_overrides).validate(
        payload, json.dumps(payload), ingestion_time=PROCESSING_TIME
    )
    return [v.rule for v in outcome.violations]


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def test_defaults_are_configured_in_de01_validation_config():
    assert THRESHOLDS.min_delay_seconds == -3600 and THRESHOLDS.max_delay_seconds == 86400
    assert THRESHOLDS.require_service_area is True and THRESHOLDS.service_area_margin_degrees == 0.05
    assert THRESHOLDS.min_timestamp == datetime(2000, 1, 1, tzinfo=UTC)
    assert THRESHOLDS.max_timestamp == datetime(2100, 1, 1, tzinfo=UTC)
    assert _rules(BASE) == []


# -- FU-08: delay_seconds bound --------------------------------------------------------


@pytest.mark.parametrize("delay", [10**30, -(10**30), 2**63, -(2**63) - 1])
def test_absurd_delay_is_rejected(delay):
    assert _rules(valid_payload(delay_seconds=delay)) == ["delay_range"]


@pytest.mark.parametrize(
    ("delay", "valid"),
    [
        (THRESHOLDS.min_delay_seconds, True),
        (THRESHOLDS.max_delay_seconds, True),
        (THRESHOLDS.min_delay_seconds - 1, False),
        (THRESHOLDS.max_delay_seconds + 1, False),
        (0, True),
    ],
)
def test_delay_bounds_are_inclusive(delay, valid):
    assert _rules(valid_payload(delay_seconds=delay)) == ([] if valid else ["delay_range"])


def test_delay_bounds_are_configurable():
    payload = valid_payload(delay_seconds=601)
    assert _rules(payload) == []
    assert _rules(payload, max_delay_seconds=600) == ["delay_range"]
    assert _rules(valid_payload(delay_seconds=-61), min_delay_seconds=-60) == ["delay_range"]


# -- FU-08: service area ---------------------------------------------------------------

MARGIN = THRESHOLDS.service_area_margin_degrees
EDGES = {
    "south": ("latitude", BOX.min_latitude - MARGIN, -1),
    "north": ("latitude", BOX.max_latitude + MARGIN, +1),
    "west": ("longitude", BOX.min_longitude - MARGIN, -1),
    "east": ("longitude", BOX.max_longitude + MARGIN, +1),
}
CENTRE = {"latitude": (BOX.min_latitude + BOX.max_latitude) / 2,
          "longitude": (BOX.min_longitude + BOX.max_longitude) / 2}


def test_null_island_is_rejected():
    assert _rules(valid_payload(latitude=0.0, longitude=0.0)) == ["service_area"]


@pytest.mark.parametrize("edge", sorted(EDGES))
@pytest.mark.parametrize(("nudge", "valid"), [(0.0, True), (-1e-6, True), (1e-6, False)])
def test_points_at_and_just_inside_or_outside_each_edge(edge, nudge, valid):
    field, boundary, outward = EDGES[edge]
    payload = valid_payload(**{**CENTRE, field: boundary + outward * nudge})
    assert _rules(payload) == ([] if valid else ["service_area"])


def test_service_area_margin_is_configurable():
    just_outside_box = valid_payload(**{**CENTRE, "latitude": BOX.min_latitude - 0.01})
    assert _rules(just_outside_box) == []  # inside the default 0.05 degree margin
    assert _rules(just_outside_box, service_area_margin_degrees=0.0) == ["service_area"]
    assert _rules(valid_payload(**{**CENTRE, "longitude": BOX.max_longitude + 0.5}),
                  service_area_margin_degrees=1.0) == []


def test_service_area_rule_can_be_switched_off():
    assert _rules(valid_payload(latitude=0.0, longitude=0.0), require_service_area=False) == []


def test_without_a_service_area_the_validator_skips_the_rule():
    # Backward compatible: TelemetryValidator without `service_area` behaves as before DE-08.
    plain = TelemetryValidator(THRESHOLDS, ROUTES)
    payload = valid_payload(latitude=0.0, longitude=0.0)
    assert plain.validate(payload, json.dumps(payload), ingestion_time=PROCESSING_TIME).is_valid


def test_de03_invalid_coordinates_keep_valid_coordinates_as_primary_rule():
    # DE-03 `invalid_coordinates` writes (999, -999): a structural failure, so
    # the stage-2 service-area rule is never evaluated for it.
    assert _rules(valid_payload(latitude=999.0, longitude=-999.0)) == ["valid_coordinates"]
    assert _rules(valid_payload(latitude=-90.5)) == ["valid_coordinates"]


def test_service_area_precedes_other_threshold_rules():
    assert _rules(valid_payload(latitude=0.0, longitude=0.0, speed_kmh=500.0, delay_seconds=10**30)) == [
        "service_area", "max_speed", "delay_range",
    ]


# -- FU-10: absolute timestamp bounds -------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"event_time": "9999-12-31T23:59:59Z"}, ["timestamp_bounds", "future_time_tolerance"]),
        ({"event_time": "9999-12-31T23:59:59Z", "generated_at": "9999-12-31T23:59:59Z"}, ["timestamp_bounds"]),
        ({"event_time": "0001-01-01T00:00:00Z"}, ["timestamp_bounds"]),
        ({"event_time": "0001-01-01T00:00:00+14:00", "generated_at": "0001-01-01T00:00:00+14:00"},
         ["timestamp_bounds"]),
        ({"generated_at": "9999-12-31T23:59:59Z"}, ["timestamp_bounds"]),
        ({"generated_at": "0001-01-01T00:00:00Z"}, ["timestamp_bounds", "future_time_tolerance"]),
    ],
)
def test_extreme_dates_are_rejected(overrides, expected):
    assert _rules(valid_payload(**overrides)) == expected


@pytest.mark.parametrize("bound", ["min_timestamp", "max_timestamp"])
def test_timestamp_bounds_are_inclusive(bound):
    at = getattr(THRESHOLDS, bound)
    assert _rules(valid_payload(event_time=_iso(at), generated_at=_iso(at))) == []
    outward = timedelta(microseconds=-1 if bound == "min_timestamp" else 1)
    beyond = _iso(at + outward)
    assert _rules(valid_payload(event_time=beyond, generated_at=beyond)) == ["timestamp_bounds"]


def test_timestamp_bounds_are_configurable():
    payload = valid_payload()  # 2025-01-01
    assert _rules(payload, min_timestamp=datetime(2025, 6, 1, tzinfo=UTC)) == ["timestamp_bounds"]
    assert _rules(payload, max_timestamp=datetime(2024, 6, 1, tzinfo=UTC)) == ["timestamp_bounds"]


@pytest.mark.parametrize(
    "update",
    [
        {"min_delay_seconds": 10, "max_delay_seconds": 5},
        {"service_area_margin_degrees": -0.1},
        {"min_timestamp": "2000-01-01T00:00:00"},  # naive
        {"max_timestamp": "2100-01-01T00:00:00"},  # naive
        {"min_timestamp": "2100-01-01T00:00:00Z", "max_timestamp": "2000-01-01T00:00:00Z"},
        {"max_timestamp": "9999-12-31T00:00:00Z"},  # in-bounds instants must stay representable
        {"min_timestamp": "0001-01-01T00:00:00Z"},
    ],
)
def test_invalid_bounds_config_is_rejected(update):
    data = THRESHOLDS.model_dump(mode="json")
    data.update(update)
    with pytest.raises(ValidationError):
        ValidationThresholds.model_validate(data)


# -- end to end: Bronze -> Silver ------------------------------------------------------

#: One line per new rule; each must be quarantined under it and the run must commit.
E2E_CASES = [
    ("delay_plus_1e30", {"delay_seconds": 10**30}, "delay_range"),
    ("delay_minus_1e30", {"delay_seconds": -(10**30)}, "delay_range"),
    ("null_island", {"latitude": 0.0, "longitude": 0.0}, "service_area"),
    ("year_9999", {"event_time": "9999-12-31T23:59:59Z", "generated_at": "9999-12-31T23:59:59Z"},
     "timestamp_bounds"),
    ("year_0001", {"event_time": "0001-01-01T00:00:00Z", "generated_at": "0001-01-01T00:00:00Z"},
     "timestamp_bounds"),
    # A UTC+14 instant in year 1 is not representable in UTC: before DE-08 this
    # crashed the Silver run while standardising the clean record.
    ("year_0001_plus14", {"event_time": "0001-01-01T00:00:00+14:00", "generated_at": "0001-01-01T00:00:00+14:00"},
     "timestamp_bounds"),
]


def test_new_rules_end_to_end(tmp_path):
    good = valid_payload()
    lines = [json.dumps(good)] + [
        json.dumps(valid_payload(event_id=f"EVT-{name}", **overrides)) for name, overrides, _ in E2E_CASES
    ]
    result = lines_to_silver(tmp_path, lines)
    counters = result.manifest["counters"]
    assert result.manifest["reconciliation"]["holds"]
    assert (counters["valid_loaded"], counters["quarantined"]) == (1, len(E2E_CASES))
    quarantine = {r["event_id"]: r for r in table(result, "quarantine")}
    for (name, _, rule), line in zip(E2E_CASES, lines[1:], strict=True):
        record = quarantine[f"EVT-{name}"]
        assert record["primary_rule"] == rule, name
        assert record["rejection_category"] == "rule"
        assert record["raw_payload_text"] == line  # lossless, big integers included
    for rule in ("delay_range", "service_area", "timestamp_bounds"):
        assert counters["quarantined_by_rule"][rule] == sum(1 for _, _, r in E2E_CASES if r == rule)
    # Nothing out of bounds reached the clean table, and the watermark is untouched.
    (clean,) = table(result, "clean")
    assert clean["event_id"] == good["event_id"]
    assert result.manifest["event_time"]["max_event_time"].startswith("2025-01-01")


def test_every_clean_event_time_is_within_the_bounds(tmp_path):
    instants = ["1999-12-31T23:59:59.999999Z", "2000-01-01T00:00:00Z", "2025-01-01T00:00:00Z",
                "2100-01-01T00:00:00Z", "2100-01-01T00:00:00.000001Z", "9999-12-31T23:59:59Z"]
    lines = [json.dumps(valid_payload(event_id=f"EVT-{i}", event_time=t, generated_at=t))
             for i, t in enumerate(instants)]
    result = lines_to_silver(tmp_path, lines)
    # Only the in-bounds instants could load (the 2100 one is also > the
    # ingestion time, so the FU-06 guard quarantines it - checked separately).
    for record in table(result, "clean"):
        event_time = datetime.fromisoformat(record["event_time"])
        assert THRESHOLDS.min_timestamp <= event_time <= THRESHOLDS.max_timestamp
    by_rule = result.manifest["counters"]["quarantined_by_rule"]
    assert by_rule["timestamp_bounds"] == 3
    assert result.manifest["counters"]["valid_loaded"] == 2


def test_manifest_rule_catalogue_lists_the_new_rules_in_precedence_order(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload())])
    rules = [r["rule"] for r in result.manifest["rule_catalogue"]]
    assert rules == [
        "bronze_parse_error", "supported_schema_version", "required_fields", "parsable_timestamps",
        "schema_conformance", "valid_coordinates", "service_area", "non_negative_speed", "max_speed",
        "non_negative_occupancy", "max_occupancy", "heading_range", "delay_range", "known_route",
        "timestamp_bounds", "future_time_tolerance", "watermark_guard",
    ]
    counters = result.manifest["counters"]
    assert set(rules) == set(counters["quarantined_by_rule"]) == set(counters["rule_violations"])
    assert result.manifest["service_area"] == {
        "bounding_box": BOX.model_dump(), "margin_degrees": MARGIN, "enabled": True,
    }
