"""DE-08 / BACKLOG FU-06: one valid-looking but far-future event must not move
the Silver event-time watermark and turn later events "late".

QA's reproduction (PR #10 D3): a single event whose `generated_at` and
`event_time` are both far in the future - so it passes the future-time
tolerance (measured against its own `generated_at`) and, here, is also
inside the FU-10 absolute timestamp bounds - advanced the watermark and
mis-flagged most later events as late (259 vs 22). The regression test
below compares every other event's late/out-of-order flag with a baseline
computed WITHOUT Silver code (`_silver_helpers.derive_expectations`: the
DE-03 structured incident log + a clean DE-02 generator + delivered event_id
order) over the same stream without the outlier.

The guard (`[silver.watermark_guard]`): an event whose event_time is more
than `max_skew_seconds` after its arrival time (`reference`: Bronze
`ingestion_time` by default, or the Silver `processing_time`) is quarantined
under rule `watermark_guard` (`mode = "quarantine"`, default), or loaded with
`_silver_watermark_guarded = true` without advancing the watermark (`mode =
"flag"`), or not checked (`mode = "off"`)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache

import pytest
from pydantic import ValidationError

from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    IncidentsConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
)
from simulator.silver import IN_ORDER, LATE, EventTimeTracker, SilverConfig
from simulator.tests._bronze_helpers import FIXED_TIME
from simulator.tests._silver_helpers import (
    PROCESSING_TIME,
    SILVER_CONFIG,
    THRESHOLDS,
    delivered_event_ids,
    derive_expectations,
    lines_to_silver,
    run_incident_stream,
    table,
    valid_payload,
)

#: A DE-03 stream with every event-time-relevant scenario enabled.
STREAM_CONFIG = IncidentsConfig(
    seed=60606,
    malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.05),
    duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.05),
    late_event=LateEventConfig(enabled=True, probability_per_tick=0.03),
    out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.2, hold_ticks=3),
    congestion=CongestionConfig(enabled=True, route_id="ROUTE-01",
                                window=RecoveryWindow(start_tick=50, duration_ticks=300)),
    breakdown=BreakdownConfig(enabled=True, vehicle_ids=["VEH-0003"],
                              window=RecoveryWindow(start_tick=0, duration_ticks=400)),
)
MAX_EVENTS = 1000
OUTLIER_ID = "EVT-FU06-OUTLIER"
#: Far in the future, but inside the default FU-10 bounds (max 2100-01-01).
OUTLIER_TIME = "2090-01-01T00:00:00Z"
#: Early in the stream, so (unguarded) nearly every later event turns late.
OUTLIER_POSITION = 25


@lru_cache(maxsize=1)
def _stream():
    return run_incident_stream(STREAM_CONFIG, MAX_EVENTS)


def _baseline():
    """Outlier-free expectation, computed without any Silver code."""
    run = _stream()
    return derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks)


def _outlier_line() -> str:
    payload = valid_payload(event_id=OUTLIER_ID, generated_at=OUTLIER_TIME, event_time=OUTLIER_TIME)
    return json.dumps(payload)


def _lines_with_outlier() -> list[str]:
    lines = list(_stream().lines)
    return lines[:OUTLIER_POSITION] + [_outlier_line()] + lines[OUTLIER_POSITION:]


def _config(**guard) -> SilverConfig:
    data = SILVER_CONFIG.model_dump(mode="json")
    data["watermark_guard"] = {**data.get("watermark_guard", {}), **guard}
    return SilverConfig.model_validate(data)


def _statuses(result) -> dict[str, str]:
    return {r["event_id"]: r["_silver_event_time_status"] for r in table(result, "clean") if r["event_id"] != OUTLIER_ID}


def _assert_flags_equal_baseline(result, baseline) -> None:
    got = _statuses(result)
    assert set(got) == baseline.valid_ids
    changed = sorted(event_id for event_id, status in got.items() if status != baseline.status_of[event_id])
    late = sum(1 for status in got.values() if status == LATE)
    assert not changed, (
        f"{len(changed)} events' event-time flags differ from the outlier-free baseline "
        f"({late} late vs {baseline.late} in the baseline); first: {changed[:5]}"
    )
    counters = result.manifest["counters"]
    assert (counters["late"], counters["out_of_order"]) == (baseline.late, baseline.out_of_order)


def test_the_stream_exercises_late_and_out_of_order_flags():
    baseline = _baseline()
    assert baseline.late > 0 and baseline.out_of_order > 0 and baseline.quarantined > 0
    assert baseline.duplicates_excluded > 0


def test_fu06_far_future_outlier_does_not_change_any_other_events_flags(tmp_path):
    """The FU-06 regression test, with the default configuration."""
    baseline = _baseline()
    result = lines_to_silver(tmp_path, _lines_with_outlier())
    _assert_flags_equal_baseline(result, baseline)

    # The outlier itself: quarantined under the named rule, and ONLY that rule
    # (it is inside the FU-10 bounds and passes the generated_at future check).
    (outlier,) = [r for r in table(result, "quarantine") if r["event_id"] == OUTLIER_ID]
    assert outlier["primary_rule"] == "watermark_guard"
    assert outlier["rejection_rules"] == ["watermark_guard"]
    assert outlier["rejection_category"] == "rule"
    counters = result.manifest["counters"]
    assert counters["quarantined_by_rule"]["watermark_guard"] == 1
    assert counters["valid_loaded"] == baseline.valid_loaded
    assert counters["quarantined"] == baseline.quarantined + 1
    assert result.manifest["event_time"]["max_event_time"] < "2026"
    assert result.manifest["reconciliation"]["holds"]


def test_fu06_flag_mode_loads_the_outlier_flagged_without_advancing_the_watermark(tmp_path):
    baseline = _baseline()
    result = lines_to_silver(tmp_path, _lines_with_outlier(), config=_config(mode="flag"))
    _assert_flags_equal_baseline(result, baseline)
    clean = table(result, "clean")
    (outlier,) = [r for r in clean if r["event_id"] == OUTLIER_ID]
    assert outlier["_silver_watermark_guarded"] is True
    assert outlier["_silver_event_time_status"] == IN_ORDER  # classified, but did not advance
    assert all(r["_silver_watermark_guarded"] is False for r in clean if r["event_id"] != OUTLIER_ID)
    counters = result.manifest["counters"]
    assert counters["watermark_guarded"] == 1
    assert counters["quarantined_by_rule"]["watermark_guard"] == 0
    assert counters["valid_loaded"] == baseline.valid_loaded + 1
    assert result.manifest["event_time"]["max_event_time"] < "2026"
    assert result.manifest["reconciliation"]["holds"]


def test_guard_off_reproduces_the_fu06_poisoning(tmp_path):
    """What the guard prevents (and what main did before DE-08): with the
    guard off the outlier is loaded and nearly every later valid event is
    flagged late."""
    baseline = _baseline()
    result = lines_to_silver(tmp_path, _lines_with_outlier(), config=_config(mode="off"))
    counters = result.manifest["counters"]
    later_valid = baseline.valid_loaded - OUTLIER_POSITION
    assert counters["late"] > baseline.late + later_valid // 2
    assert counters["quarantined_by_rule"]["watermark_guard"] == 0
    assert result.manifest["event_time"]["max_event_time"].startswith("2090")


@pytest.mark.parametrize("mode", ["quarantine", "flag", "off"])
def test_without_an_outlier_the_guard_changes_nothing(tmp_path, mode):
    """Criterion 6: with no outlier, every mode gives the oracle's result and
    byte-identical tables."""
    baseline = _baseline()
    result = lines_to_silver(tmp_path / mode, list(_stream().lines), config=_config(mode=mode))
    _assert_flags_equal_baseline(result, baseline)
    counters = result.manifest["counters"]
    assert counters["watermark_guarded"] == 0 and counters["quarantined_by_rule"]["watermark_guard"] == 0
    assert counters["valid_loaded"] == baseline.valid_loaded
    assert counters["quarantined"] == baseline.quarantined
    assert counters["duplicates_excluded"] == baseline.duplicates_excluded
    reference = lines_to_silver(tmp_path / "reference", list(_stream().lines))
    for role in ("clean", "quarantine", "duplicates"):
        assert table(result, role) == table(reference, role), role


# -- boundaries, reference point, configuration -----------------------------------------


def _at(base: datetime, seconds: float) -> str:
    return (base + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


@pytest.mark.parametrize(
    ("seconds_after_ingestion", "quarantined"),
    [(0, False), (300, False), (300.000001, True), (3600, True), (-86400, False)],
)
def test_guard_boundary_against_bronze_ingestion_time(tmp_path, seconds_after_ingestion, quarantined):
    # Bronze ingestion_time in the helpers is FIXED_TIME; default max_skew_seconds = 300.
    assert SILVER_CONFIG.watermark_guard.max_skew_seconds == 300
    when = _at(FIXED_TIME, seconds_after_ingestion)
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload(event_time=when, generated_at=when))])
    counters = result.manifest["counters"]
    assert counters["quarantined_by_rule"]["watermark_guard"] == int(quarantined)
    assert counters["valid_loaded"] == int(not quarantined)


def test_guard_reference_is_configurable(tmp_path):
    # 20 minutes after ingestion (09:30), but 10 minutes before processing (10:00).
    when = _at(FIXED_TIME, 1200)
    assert FIXED_TIME < datetime.fromisoformat(when) < PROCESSING_TIME
    line = json.dumps(valid_payload(event_time=when, generated_at=when))
    by_ingestion = lines_to_silver(tmp_path / "i", [line])
    assert by_ingestion.manifest["counters"]["quarantined_by_rule"]["watermark_guard"] == 1
    (record,) = table(by_ingestion, "quarantine")
    assert "ingestion_time" in record["rejections"][0]["reason"]
    by_processing = lines_to_silver(tmp_path / "p", [line], config=_config(reference="processing_time"))
    assert by_processing.manifest["counters"]["valid_loaded"] == 1
    wider = lines_to_silver(tmp_path / "w", [line], config=_config(max_skew_seconds=1200))
    assert wider.manifest["counters"]["valid_loaded"] == 1


def test_each_delivery_of_an_outlier_is_quarantined(tmp_path):
    line = _outlier_line()
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload()), line, line])
    counters = result.manifest["counters"]
    assert counters["quarantined_by_rule"]["watermark_guard"] == 2
    assert counters["duplicates_excluded"] == 0 and counters["valid_loaded"] == 1


def test_guard_is_recorded_in_the_manifest_and_fingerprint(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload())])
    assert result.manifest["config"]["watermark_guard"] == {
        "mode": "quarantine", "reference": "ingestion_time", "max_skew_seconds": 300,
    }
    assert result.manifest["event_time"]["watermark_guard"] == result.manifest["config"]["watermark_guard"]
    assert "watermark_guard" in [r["rule"] for r in result.manifest["rule_catalogue"]]
    other = lines_to_silver(tmp_path / "flag", [json.dumps(valid_payload())], config=_config(mode="flag"))
    assert other.manifest["input_fingerprint"] != result.manifest["input_fingerprint"]


@pytest.mark.parametrize(
    "guard",
    [{"mode": "drop"}, {"reference": "generated_at"}, {"max_skew_seconds": -1}, {"surprise": 1}],
)
def test_invalid_guard_config_is_rejected(guard):
    with pytest.raises(ValidationError):
        _config(**guard)


def test_tracker_can_classify_without_advancing():
    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    tracker = EventTimeTracker(allowed_lateness_seconds=60)
    tracker.observe(t0)
    assert tracker.observe(t0 + timedelta(days=365 * 60), advance=False)[0] == IN_ORDER
    assert tracker.max_event_time == t0
    assert tracker.observe(t0 + timedelta(seconds=1))[0] == IN_ORDER  # not late


def test_outlier_is_inside_the_fu10_bounds():
    """The reproduction is meaningful only if the absolute bounds do not
    already reject it (FU-10 removes absurd timestamps; FU-06 is about
    plausible-but-wrong ones)."""
    outlier = datetime.fromisoformat(OUTLIER_TIME)
    assert THRESHOLDS.min_timestamp <= outlier <= THRESHOLDS.max_timestamp
