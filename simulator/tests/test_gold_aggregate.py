"""DE-07 Gold aggregate: hand-computed fixture, exclusion of quarantined and
duplicate records, inclusion of late rows, period/timezone bucketing and the
quarantine attribution rule.

The expected numbers live in `_gold_helpers` (EXPECTED_ROWS / EXPECTED_TOTALS)
and were computed by hand from the fixture table in its docstring."""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from simulator.gold import COLUMNS, GoldConfig, Periods, attribute_quarantined
from simulator.gold.aggregate import (
    UNATTRIBUTED_BRONZE_PARSE_ERROR,
    UNATTRIBUTED_EVENT_TIME_REJECTED,
    UNATTRIBUTED_EVENT_TIME_UNPARSEABLE,
    UNATTRIBUTED_PAYLOAD_UNAVAILABLE,
    UNATTRIBUTED_ROUTE_UNKNOWN,
)
from simulator.tests._gold_helpers import (
    EXPECTED_ROWS,
    EXPECTED_TOTALS,
    GOLD_CONFIG,
    fixture_silver,
    gold,
    gold_rows,
    silver_from_lines,
    silver_table,
)
from simulator.tests._silver_helpers import valid_payload

HOURLY_AEST = Periods(period_minutes=60, utc_offset_hours=10)


# -- the hand-computed fixture ---------------------------------------------------


def test_fixture_silver_outcome_is_what_the_hand_computation_assumes(tmp_path):
    """Guard: the fixture really produces the Silver buckets the hand
    computation is based on (otherwise the expected rows would be moot)."""
    counters = fixture_silver(tmp_path).manifest["counters"]
    for key in ("extracted", "valid_loaded", "quarantined", "duplicates_excluded", "late"):
        assert counters[key] == EXPECTED_TOTALS[key], key
    result = fixture_silver(tmp_path / "again")
    clean = {r["event_id"]: r for r in silver_table(result, "clean")}
    assert sorted(clean) == ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"]
    assert {e for e, r in clean.items() if r["_silver_event_time_status"] == "late"} == {"E5", "E8"}
    quarantine = {r["event_id"]: r["primary_rule"] for r in silver_table(result, "quarantine")}
    assert quarantine == {
        "Q1": "non_negative_speed", "Q2": "required_fields", "Q3": "known_route", "Q4": "parsable_timestamps",
        None: "bronze_parse_error", "Q7": "schema_conformance", "Q5": "future_time_tolerance",
        "Q6": "non_negative_occupancy",
    }


def test_aggregate_matches_hand_computed_rows(tmp_path):
    fixture_silver(tmp_path)
    result = gold(tmp_path)
    assert result.status == "COMMITTED"
    assert gold_rows(result) == EXPECTED_ROWS


def test_aggregate_matches_hand_computed_totals(tmp_path):
    fixture_silver(tmp_path)
    counters = gold(tmp_path).manifest["counters"]
    for key in ("quarantined_attributed", "quarantined_unattributed", "unattributed_by_reason",
                "overall_valid_percentage"):
        assert counters[key] == EXPECTED_TOTALS[key], key
    assert counters["event_count"] == EXPECTED_TOTALS["valid_loaded"]
    assert counters["duplicates_excluded"] == EXPECTED_TOTALS["duplicates_excluded"]
    assert counters["rows"] == len(EXPECTED_ROWS)
    assert counters["routes"] == 3 and counters["periods"] == 4


def test_csv_holds_the_same_rows_as_the_jsonl(tmp_path):
    fixture_silver(tmp_path)
    result = gold(tmp_path)
    entry = result.manifest["tables"]["route_performance"]
    text = (tmp_path / "gold" / entry["csv_path"]).read_text(encoding="utf-8")
    parsed = list(csv.DictReader(io.StringIO(text)))
    assert tuple(parsed[0].keys()) == COLUMNS
    expected = [{k: "" if v is None else str(v) for k, v in row.items()} for row in EXPECTED_ROWS]
    assert parsed == expected


def test_csv_can_be_disabled(tmp_path):
    fixture_silver(tmp_path)
    result = gold(tmp_path, config=GOLD_CONFIG.model_copy(update={"write_csv": False}))
    entry = result.manifest["tables"]["route_performance"]
    assert entry["csv_path"] is None
    assert not list((tmp_path / "gold").rglob("*.csv"))
    assert gold_rows(result) == EXPECTED_ROWS


# -- exclusion / inclusion ---------------------------------------------------------


def test_quarantined_and_duplicate_values_never_reach_counts_or_averages(tmp_path):
    """Every quarantined record carries delay 5000 and the excluded duplicate
    copy of E2 carries 9999; neither value may appear in any sum."""
    fixture_silver(tmp_path)
    rows = gold_rows(gold(tmp_path))
    assert sum(r["sum_delay_seconds"] for r in rows) == 60 + 120 + 30 + 300 + 100 + 0 + 45 + 15
    r01_h00 = rows[0]
    assert (r01_h00["route_id"], r01_h00["period_start"]) == ("ROUTE-01", "2025-01-01T00:00:00.000000Z")
    # E2 counted once with its FIRST (kept) value 120, Q1 (5000) not averaged in.
    assert r01_h00["event_count"] == 3 and r01_h00["sum_delay_seconds"] == 280
    assert r01_h00["quarantined_count"] == 1
    # A quarantine-only cell has no valid events and no average.
    r03 = [r for r in rows if r["route_id"] == "ROUTE-03"]
    assert r03 == [EXPECTED_ROWS[-1]]
    assert r03[0]["event_count"] == 0 and r03[0]["avg_delay_seconds"] is None


def test_a_cell_holding_only_a_duplicate_produces_no_extra_row(tmp_path):
    first = valid_payload(event_id="D1", route_id="ROUTE-05", event_time="2025-01-01T05:10:00Z",
                          generated_at="2025-01-01T05:10:00Z", trip_id="TRIP-ROUTE-05-0000", delay_seconds=10)
    # Same event_id, different route/time/delay: a conflicting replay. If it
    # were counted it would open a ROUTE-06 07:00 row.
    replay = dict(first, route_id="ROUTE-06", event_time="2025-01-01T07:10:00Z",
                  generated_at="2025-01-01T07:10:00Z", delay_seconds=777)
    silver_from_lines(tmp_path, [json.dumps(first), json.dumps(replay)])
    result = gold(tmp_path)
    rows = gold_rows(result)
    assert [(r["route_id"], r["period_start"], r["event_count"], r["avg_delay_seconds"]) for r in rows] == [
        ("ROUTE-05", "2025-01-01T05:00:00.000000Z", 1, 10.0)
    ]
    assert result.manifest["counters"]["duplicates_excluded"] == 1


def test_late_rows_are_included_in_counts_and_averages(tmp_path):
    fixture_silver(tmp_path)
    rows = {(r["route_id"], r["period_start"][11:13]): r for r in gold_rows(gold(tmp_path))}
    # E5 (late, delay 100) is in ROUTE-01 h00: without it the cell would be 2 events, avg 90.0.
    assert rows[("ROUTE-01", "00")]["late_event_count"] == 1
    assert rows[("ROUTE-01", "00")]["event_count"] == 3
    assert rows[("ROUTE-01", "00")]["avg_delay_seconds"] == 93.3333
    # E8 (late, delay 15, given as +10:00 local time) is in ROUTE-02 h01: without it 1 event, avg 0.0.
    assert rows[("ROUTE-02", "01")]["late_event_count"] == 1
    assert rows[("ROUTE-02", "01")]["event_count"] == 2
    assert rows[("ROUTE-02", "01")]["avg_delay_seconds"] == 7.5


def test_a_stream_of_mostly_late_events_is_aggregated_in_full(tmp_path):
    def event(i, hhmm, delay):
        t = f"2025-01-01T{hhmm}:00Z"
        return json.dumps(valid_payload(event_id=f"L{i}", route_id="ROUTE-04", event_time=t, generated_at=t,
                                        trip_id="TRIP-ROUTE-04-0000", delay_seconds=delay))

    # 06:00 first (in order), then three events far behind the watermark.
    result_silver = silver_from_lines(tmp_path, [event(0, "06:00", 1), event(1, "04:05", 2),
                                                 event(2, "04:10", 4), event(3, "04:59", 6)])
    assert result_silver.manifest["counters"]["late"] == 3
    rows = gold_rows(gold(tmp_path))
    assert [(r["period_start"][11:13], r["event_count"], r["late_event_count"], r["avg_delay_seconds"])
            for r in rows] == [("04", 3, 3, 4.0), ("06", 1, 0, 1.0)]


# -- periods / timezone ------------------------------------------------------------


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


@pytest.mark.parametrize(
    ("instant", "period_minutes", "offset", "expected_start"),
    [
        # hourly, +10: identical to UTC hours (whole-hour offset)
        ("2025-01-01T00:59:59.999999+00:00", 60, 10, "2025-01-01T00:00:00+00:00"),
        ("2025-01-01T01:00:00+00:00", 60, 10, "2025-01-01T01:00:00+00:00"),
        ("2025-01-01T11:30:00+10:00", 60, 10, "2025-01-01T01:00:00+00:00"),
        # daily, +10: local midnight 2025-01-02 00:00+10:00 == 2025-01-01T14:00Z
        ("2025-01-01T13:59:59+00:00", 1440, 10, "2024-12-31T14:00:00+00:00"),
        ("2025-01-01T14:00:00+00:00", 1440, 10, "2025-01-01T14:00:00+00:00"),
        # daily, UTC
        ("2025-01-01T23:59:59+00:00", 1440, 0, "2025-01-01T00:00:00+00:00"),
        # 15-minute buckets, negative offset, before the 1970 epoch
        ("1969-12-31T23:07:00+00:00", 15, -5, "1969-12-31T23:00:00+00:00"),
    ],
)
def test_period_bucketing(instant, period_minutes, offset, expected_start):
    periods = Periods(period_minutes=period_minutes, utc_offset_hours=offset)
    assert periods.start_of(_utc(instant)) == _utc(expected_start)


@pytest.mark.parametrize(
    ("periods", "instant"),
    [
        # Local +10 pushes the bucket start past datetime.max.
        (HOURLY_AEST, "9999-12-31T20:00:00+00:00"),
        # Start is representable in UTC (23:00Z) but the bucket END is not.
        (Periods(period_minutes=60, utc_offset_hours=0), "9999-12-31T23:30:00+00:00"),
        # A negative offset pushes the bucket before datetime.min.
        (Periods(period_minutes=60, utc_offset_hours=-10), "0001-01-01T05:00:00+00:00"),
    ],
)
def test_unrepresentable_bucket_raises_overflow(periods, instant):
    with pytest.raises(OverflowError):
        periods.start_of(_utc(instant))


def test_period_labels_are_utc_and_local():
    labels = HOURLY_AEST.labels(_utc("2025-01-01T23:00:00+00:00"))
    assert labels == {
        "period_start": "2025-01-01T23:00:00.000000Z",
        "period_end": "2025-01-02T00:00:00.000000Z",
        "period_start_local": "2025-01-02T09:00:00.000000+10:00",
        "period_end_local": "2025-01-02T10:00:00.000000+10:00",
        "period_minutes": 60,
    }
    assert HOURLY_AEST.local_tz == timezone(timedelta(hours=10))


def test_daily_periods_change_the_aggregate(tmp_path):
    fixture_silver(tmp_path)
    config = GOLD_CONFIG.model_copy(update={"period_minutes": 1440})
    rows = gold_rows(gold(tmp_path, config=config))
    # All fixture instants (00:10Z .. 03:10Z) fall in local day 2025-01-01 (10:00..13:10 +10:00).
    assert {r["period_start"] for r in rows} == {"2024-12-31T14:00:00.000000Z"}
    by_route = {r["route_id"]: r for r in rows}
    # ROUTE-01: E1 60 + E2 120 + E5 100 + E4 300 = 580 / 4 = 145.0, Q1 -> 4/5 = 80.0 %
    assert (by_route["ROUTE-01"]["event_count"], by_route["ROUTE-01"]["avg_delay_seconds"],
            by_route["ROUTE-01"]["valid_percentage"]) == (4, 145.0, 80.0)
    # ROUTE-02: E3 30 + E6 0 + E7 45 + E8 15 = 90 / 4 = 22.5, Q2 -> 4/5 = 80.0 %
    assert (by_route["ROUTE-02"]["event_count"], by_route["ROUTE-02"]["avg_delay_seconds"],
            by_route["ROUTE-02"]["valid_percentage"]) == (4, 22.5, 80.0)


@pytest.mark.parametrize("bad", [{"period_minutes": 7}, {"period_minutes": 0}, {"utc_offset_hours": 15},
                                 {"route_performance_table": "../x"}, {"unknown_key": 1}])
def test_config_is_validated(bad):
    base = GOLD_CONFIG.model_dump()
    with pytest.raises(ValidationError):
        GoldConfig.model_validate({**base, **bad})


# -- attribution rule (unit) -------------------------------------------------------

KNOWN = frozenset({"ROUTE-01"})


def _q(payload=None, *, raw_format="json_object", rules=("non_negative_speed",)):
    return {"raw_format": raw_format, "raw_payload": payload, "rejection_rules": list(rules)}


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (_q(None, raw_format="bronze_parse_error", rules=("bronze_parse_error",)), UNATTRIBUTED_BRONZE_PARSE_ERROR),
        (_q(None, rules=("schema_conformance",)), UNATTRIBUTED_PAYLOAD_UNAVAILABLE),
        (_q({"event_time": "2025-01-01T00:00:00Z"}), UNATTRIBUTED_ROUTE_UNKNOWN),
        (_q({"route_id": None, "event_time": "2025-01-01T00:00:00Z"}), UNATTRIBUTED_ROUTE_UNKNOWN),
        (_q({"route_id": 1, "event_time": "2025-01-01T00:00:00Z"}), UNATTRIBUTED_ROUTE_UNKNOWN),
        (_q({"route_id": "ROUTE-99", "event_time": "2025-01-01T00:00:00Z"}), UNATTRIBUTED_ROUTE_UNKNOWN),
        (_q({"route_id": "ROUTE-01"}), UNATTRIBUTED_EVENT_TIME_UNPARSEABLE),
        (_q({"route_id": "ROUTE-01", "event_time": "2025-01-01T00:00:00"}), UNATTRIBUTED_EVENT_TIME_UNPARSEABLE),
        (_q({"route_id": "ROUTE-01", "event_time": 1735689600}), UNATTRIBUTED_EVENT_TIME_UNPARSEABLE),
        (_q({"route_id": "ROUTE-01", "event_time": "0001-01-01T00:00:00+14:00"}),
         UNATTRIBUTED_EVENT_TIME_UNPARSEABLE),
        (_q({"route_id": "ROUTE-01", "event_time": "2025-01-01T00:00:00Z"}, rules=("future_time_tolerance",)),
         UNATTRIBUTED_EVENT_TIME_REJECTED),
        (_q({"route_id": "ROUTE-01", "event_time": "2025-01-01T10:30:00+10:00"}),
         ("ROUTE-01", _utc("2025-01-01T00:00:00+00:00"))),
        (_q({"route_id": "ROUTE-01", "vehicle_id": None, "event_time": "2025-01-01T00:59:00Z"},
            rules=("required_fields",)), ("ROUTE-01", _utc("2025-01-01T00:00:00+00:00"))),
    ],
)
def test_quarantine_attribution_rule(record, expected):
    assert attribute_quarantined(record, KNOWN, HOURLY_AEST) == expected
