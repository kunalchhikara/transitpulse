"""DE-06 event time vs processing time: the event-time watermark (max valid
event_time seen in arrival order minus allowed lateness), late events loaded
and flagged (never dropped), out-of-order events within lateness accepted and
flagged, processing_time from the injected clock."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from simulator.silver import IN_ORDER, LATE, OUT_OF_ORDER, EventTimeTracker
from simulator.tests._silver_helpers import (
    PROCESSING_TIME_STR,
    SILVER_CONFIG,
    clean_payloads,
    lines_to_silver,
    table,
)

T0 = datetime(2025, 1, 1, tzinfo=UTC)


def _at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _iso(seconds: float) -> str:
    return _at(seconds).isoformat().replace("+00:00", "Z")


def test_tracker_classifies_in_order_out_of_order_and_late_at_the_boundary():
    tracker = EventTimeTracker(allowed_lateness_seconds=60)
    assert tracker.observe(_at(100)) == (IN_ORDER, 0.0, None)
    assert tracker.observe(_at(100)) == (IN_ORDER, 0.0, _at(40))  # equal to max -> in order
    assert tracker.observe(_at(110))[0] == IN_ORDER
    assert tracker.observe(_at(105)) == (OUT_OF_ORDER, 5.0, _at(50))
    assert tracker.observe(_at(50)) == (OUT_OF_ORDER, 60.0, _at(50))  # exactly at watermark: accepted
    assert tracker.observe(_at(49.999)) == (LATE, 60.001, _at(50))
    # Late/out-of-order events never move the watermark backwards or forwards.
    assert tracker.watermark == _at(50)
    assert tracker.max_event_time == _at(110) and tracker.min_event_time == _at(49.999)


def test_zero_allowed_lateness_makes_every_out_of_order_event_late():
    tracker = EventTimeTracker(allowed_lateness_seconds=0)
    tracker.observe(_at(10))
    assert tracker.observe(_at(9.9))[0] == LATE


def _stream(offsets_and_skews):
    """One clean DE-02 payload per (tick, event_time offset from generated_at)."""
    payloads = clean_payloads(len(offsets_and_skews))
    lines = []
    for payload, skew in zip(payloads, offsets_and_skews, strict=True):
        generated = datetime.fromisoformat(payload["generated_at"])
        payload["event_time"] = (generated - timedelta(seconds=skew)).isoformat().replace("+00:00", "Z")
        lines.append(json.dumps(payload))
    return payloads, lines


def test_late_event_is_loaded_flagged_and_counted_not_dropped(tmp_path):
    # 10 events/sec: tick 5 arrives with event_time 300 s before its send time.
    skews = [0, 0, 0, 0, 0, 300, 0, 0]
    payloads, lines = _stream(skews)
    result = lines_to_silver(tmp_path, lines)
    counters = result.manifest["counters"]
    assert (counters["valid_loaded"], counters["late"], counters["quarantined"]) == (8, 1, 0)
    clean = table(result, "clean")
    late = [r for r in clean if r["_silver_event_time_status"] == LATE]
    assert [r["event_id"] for r in late] == [payloads[5]["event_id"]]
    # lateness = max event_time seen (tick 4 = 0.4 s) - event_time (0.5 - 300 s)
    assert abs(late[0]["_silver_lateness_seconds"] - 299.9) < 1e-6
    assert late[0]["_silver_watermark"] == "2024-12-31T23:59:00.400000Z"
    assert late[0]["event_time"] == "2024-12-31T23:55:00.500000Z"  # event time kept, standardised
    # The late event did not advance the watermark: the next one is in order.
    assert clean[6]["_silver_event_time_status"] == IN_ORDER


def test_out_of_order_within_allowed_lateness_is_accepted_and_flagged(tmp_path):
    payloads = clean_payloads(4)
    order = [0, 2, 1, 3]  # tick 2 overtakes tick 1 (0.1 s)
    result = lines_to_silver(tmp_path, [json.dumps(payloads[i]) for i in order])
    counters = result.manifest["counters"]
    assert (counters["in_order"], counters["out_of_order"], counters["late"]) == (3, 1, 0)
    clean = table(result, "clean")
    assert [r["_silver_event_time_status"] for r in clean] == [IN_ORDER, IN_ORDER, OUT_OF_ORDER, IN_ORDER]
    assert abs(clean[2]["_silver_lateness_seconds"] - 0.1) < 1e-9
    # Arrival order (not event time) is the order of the clean table.
    assert [r["_silver_arrival_seq"] for r in clean] == [1, 2, 3, 4]


def test_invalid_future_event_does_not_poison_the_watermark(tmp_path):
    payloads = clean_payloads(3)
    future = dict(payloads[1], event_time="2030-01-01T00:00:00Z")  # quarantined: future_time_tolerance
    lines = [json.dumps(payloads[0]), json.dumps(future), json.dumps(payloads[2])]
    counters = lines_to_silver(tmp_path, lines).manifest["counters"]
    assert counters["quarantined_by_rule"]["future_time_tolerance"] == 1
    assert counters["late"] == 0 and counters["in_order"] == 2


def test_duplicates_are_removed_before_event_time_classification(tmp_path):
    payloads = clean_payloads(80)
    lines = [json.dumps(p) for p in payloads] + [json.dumps(payloads[0])]  # replay 7.9 s later
    counters = lines_to_silver(tmp_path, lines).manifest["counters"]
    assert counters["duplicates_excluded"] == 1
    assert counters["late"] == 0 and counters["out_of_order"] == 0


def test_processing_time_is_the_injected_clock_and_distinct_from_event_and_ingestion_time(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(clean_payloads(1)[0])])
    (record,) = table(result, "clean")
    assert record["processing_time"] == PROCESSING_TIME_STR
    assert record["ingestion_time"] != record["processing_time"] != record["event_time"]
    assert result.manifest["processing_time"] == PROCESSING_TIME_STR
    event_time = result.manifest["event_time"]
    assert event_time["allowed_lateness_seconds"] == SILVER_CONFIG.allowed_lateness_seconds
    assert event_time["max_event_time"] == record["event_time"]
    assert result.manifest["bronze_runs_consumed"][0]["bronze_to_silver_latency_seconds"] == 1800.0
