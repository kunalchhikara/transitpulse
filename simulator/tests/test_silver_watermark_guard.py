"""DE-08 / BACKLOG FU-06: one valid-looking but far-future event must not move
the Silver event-time watermark and turn later events "late".

QA's reproduction (PR #10 D3): a single event whose `generated_at` and
`event_time` are both far in the future - so it passes the future-time
tolerance (measured against its own `generated_at`) and, here, is also
inside the FU-10 absolute timestamp bounds - advanced the watermark and
mis-flagged most later events as late (259 vs 22). The regression tests
compare every other event's late/out-of-order flag with a baseline computed
WITHOUT Silver code (`_silver_helpers.derive_expectations`: the DE-03
structured incident log + a clean DE-02 generator + delivered event_id
order) over the same stream without the outlier.

The guard (`[silver.watermark_guard]`, README "Watermark guard") is
STREAM-relative (QA D1 of PR #16): a jump of more than `max_jump_seconds`
ahead of the current maximum must not be contradicted by the stream that
follows it. Mode `flag` (default) loads a held-back event with
`_silver_watermark_guarded = true` without advancing the watermark; `off`
disables the guard. The QA D1 scenarios (outliers before the ingestion
clock, data ahead of the ingestion clock) are in
`test_silver_watermark_guard_qa.py`. The clock-relative check survives as
the opt-in rule `arrival_time_skew` (`[silver.arrival_time_skew]`), tested at
the end of this file."""

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
from simulator.silver import (
    IN_ORDER,
    LATE,
    EventTimeSequencer,
    EventTimeTracker,
    SilverConfig,
    WatermarkGuard,
)
from simulator.tests._bronze_helpers import FIXED_TIME
from simulator.tests._silver_helpers import (
    PROCESSING_TIME,
    SILVER_CONFIG,
    THRESHOLDS,
    clean_payloads,
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
T0 = datetime(2025, 1, 1, tzinfo=UTC)


@lru_cache(maxsize=1)
def _stream():
    return run_incident_stream(STREAM_CONFIG, MAX_EVENTS)


def _baseline():
    """Outlier-free expectation, computed without any Silver code."""
    run = _stream()
    return derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks)


def _outlier_line(when: str = OUTLIER_TIME, event_id: str = OUTLIER_ID, **overrides) -> str:
    return json.dumps(valid_payload(event_id=event_id, generated_at=when, event_time=when, **overrides))


def _lines_with_outlier() -> list[str]:
    lines = list(_stream().lines)
    return lines[:OUTLIER_POSITION] + [_outlier_line()] + lines[OUTLIER_POSITION:]


def _config(*, arrival_time_skew: dict | None = None, **guard) -> SilverConfig:
    data = SILVER_CONFIG.model_dump(mode="json")
    data["watermark_guard"] = {**data["watermark_guard"], **guard}
    if arrival_time_skew is not None:
        data["arrival_time_skew"] = {**data["arrival_time_skew"], **arrival_time_skew}
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
    """The FU-06 regression test, with the default configuration: the outlier
    is loaded and flagged (never quarantined - it is valid by every rule) and
    does not advance the watermark."""
    baseline = _baseline()
    result = lines_to_silver(tmp_path, _lines_with_outlier())
    _assert_flags_equal_baseline(result, baseline)
    clean = table(result, "clean")
    (outlier,) = [r for r in clean if r["event_id"] == OUTLIER_ID]
    assert outlier["_silver_watermark_guarded"] is True
    assert outlier["_silver_event_time_status"] == IN_ORDER  # classified, but did not advance
    assert all(r["_silver_watermark_guarded"] is False for r in clean if r["event_id"] != OUTLIER_ID)
    counters = result.manifest["counters"]
    assert counters["watermark_guarded"] == 1
    assert counters["valid_loaded"] == baseline.valid_loaded + 1
    assert counters["quarantined"] == baseline.quarantined
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
    assert counters["watermark_guarded"] == 0
    assert result.manifest["event_time"]["max_event_time"].startswith("2090")


@pytest.mark.parametrize("mode", ["flag", "off"])
def test_without_an_outlier_the_guard_changes_nothing(tmp_path, mode):
    """Criterion 6: with no outlier, every mode gives the oracle's result and
    byte-identical tables."""
    baseline = _baseline()
    result = lines_to_silver(tmp_path / mode, list(_stream().lines), config=_config(mode=mode))
    _assert_flags_equal_baseline(result, baseline)
    counters = result.manifest["counters"]
    assert counters["watermark_guarded"] == 0 and counters["quarantined_watermark_guarded"] == 0
    assert counters["valid_loaded"] == baseline.valid_loaded
    assert counters["quarantined"] == baseline.quarantined
    assert counters["duplicates_excluded"] == baseline.duplicates_excluded
    reference = lines_to_silver(tmp_path / "reference", list(_stream().lines))
    for role in ("clean", "quarantine", "duplicates"):
        assert table(result, role) == table(reference, role), role


def test_a_quarantined_record_gets_the_guards_verdict_but_never_moves_time(tmp_path):
    """A quarantined record with a usable event_time is "probed": Silver
    records whether that time is a held-back jump (Gold then does not
    attribute it), but it is never evidence and never moves the watermark."""
    baseline = _baseline()
    lines = list(_stream().lines)
    bad_outlier = _outlier_line(event_id="EVT-BAD-OUTLIER", speed_kmh=-5.0)
    lines = lines[:OUTLIER_POSITION] + [bad_outlier] + lines[OUTLIER_POSITION:]
    result = lines_to_silver(tmp_path, lines)
    _assert_flags_equal_baseline(result, baseline)
    quarantine = table(result, "quarantine")
    (bad,) = [r for r in quarantine if r["event_id"] == "EVT-BAD-OUTLIER"]
    assert bad["_silver_watermark_guarded"] is True
    assert result.manifest["counters"]["quarantined_watermark_guarded"] == 1
    # Records whose event_time is unusable (parse errors, malformed timestamps) carry null.
    assert any(r["_silver_watermark_guarded"] is None for r in quarantine)
    assert all(r["_silver_watermark_guarded"] in (None, False) for r in quarantine if r is not bad)


def test_single_vehicle_ambiguity_keeps_the_unguarded_result(tmp_path):
    """One reading ahead followed by readings of the SAME vehicle far behind
    it is ambiguous (a glitch, or genuinely late data): one vehicle is not
    enough evidence, so the result is exactly the unguarded one (DE-07's
    'mostly late' stream)."""
    def line(i, hhmm):
        t = f"2025-01-01T{hhmm}:00Z"
        return json.dumps(valid_payload(event_id=f"L{i}", event_time=t, generated_at=t))

    lines = [line(0, "06:00"), line(1, "04:05"), line(2, "04:10"), line(3, "04:59")]
    on = lines_to_silver(tmp_path / "on", lines)
    off = lines_to_silver(tmp_path / "off", lines, config=_config(mode="off"))
    assert on.manifest["counters"]["late"] == 3 and on.manifest["counters"]["watermark_guarded"] == 0
    assert table(on, "clean") == table(off, "clean")


def test_guard_is_recorded_in_the_manifest_and_fingerprint(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload())])
    assert result.manifest["silver_manifest_version"] == "1.1.0"
    assert result.manifest["config"]["watermark_guard"] == {
        "mode": "flag", "max_jump_seconds": 900, "lookahead_events": 100,
        "min_corroborating_vehicles": 2, "min_contradicting_vehicles": 2,
    }
    assert result.manifest["config"]["arrival_time_skew"] == {
        "mode": "off", "reference": "ingestion_time", "max_skew_seconds": 300,
    }
    event_time = result.manifest["event_time"]
    assert event_time["watermark_guard"] == result.manifest["config"]["watermark_guard"]
    assert event_time["arrival_time_skew"] == result.manifest["config"]["arrival_time_skew"]
    assert "watermark_guard" not in [r["rule"] for r in result.manifest["rule_catalogue"]]
    other = lines_to_silver(tmp_path / "off", [json.dumps(valid_payload())], config=_config(mode="off"))
    assert other.manifest["input_fingerprint"] != result.manifest["input_fingerprint"]


@pytest.mark.parametrize(
    "guard",
    [{"mode": "quarantine"}, {"mode": "drop"}, {"max_jump_seconds": 0}, {"lookahead_events": 0},
     {"min_corroborating_vehicles": 0}, {"min_contradicting_vehicles": 0},
     {"lookahead_events": 1, "min_corroborating_vehicles": 2}, {"reference": "ingestion_time"},
     {"surprise": 1}],
)
def test_invalid_guard_config_is_rejected(guard):
    with pytest.raises(ValidationError):
        _config(**guard)


@pytest.mark.parametrize(
    "skew", [{"mode": "flag"}, {"reference": "generated_at"}, {"max_skew_seconds": -1}, {"surprise": 1}])
def test_invalid_arrival_time_skew_config_is_rejected(skew):
    with pytest.raises(ValidationError):
        _config(arrival_time_skew=skew)


def test_pre_de08_silver_config_without_the_new_tables_is_rejected():
    data = SILVER_CONFIG.model_dump(mode="json")
    for key in ("watermark_guard", "arrival_time_skew"):
        partial = {k: v for k, v in data.items() if k != key}
        with pytest.raises(ValidationError):
            SilverConfig.model_validate(partial)


# -- the decision rule, unit level ---------------------------------------------------------

GUARD = WatermarkGuard.from_config(SILVER_CONFIG.watermark_guard)  # 900 s, 100, 2, 2


def _at(seconds: float, base: datetime = T0) -> datetime:
    return base + timedelta(seconds=seconds)


@pytest.mark.parametrize(
    ("later", "complete", "expected"),
    [
        # Two other vehicles near or past it: corroborated -> advances (decided early).
        ([(_at(86400 + 5), "V2"), (_at(86400 - 899), "V3")], False, False),
        # The same vehicle never corroborates itself.
        ([(_at(86400 + 5), "V1"), (_at(86400 + 9), "V1")], True, False),  # ...nor contradicts: no evidence
        # Two vehicles far behind, nobody near it: held back once the window is complete.
        ([(_at(1), "V2"), (_at(2), "V3")], True, True),
        ([(_at(1), "V2"), (_at(2), "V3")], False, None),  # more evidence may still come
        # One contradicting vehicle (its own) is not enough evidence.
        ([(_at(1), "V1"), (_at(2), "V1"), (_at(3), "V1")], True, False),
        # Exactly max_jump behind is NOT contradiction (it corroborates if another vehicle).
        ([(_at(86400 - 900), "V2"), (_at(86400 - 900), "V3")], True, False),
        ([(_at(86400 - 900.000001), "V2"), (_at(86400 - 900.000001), "V3")], True, True),
        # One corroborating vehicle + contradiction from two -> held back.
        ([(_at(86400), "V2"), (_at(1), "V3"), (_at(2), "V4")], True, True),
        # Empty lookahead at the end of input: no evidence -> advances.
        ([], True, False),
    ],
)
def test_decision_rule(later, complete, expected):
    assert GUARD.decide(_at(86400), "V1", later, complete=complete) == expected


def test_decision_rule_only_looks_lookahead_events_ahead():
    guard = WatermarkGuard.from_config(_config(lookahead_events=3).watermark_guard)
    behind = [(_at(1), "V2"), (_at(2), "V3"), (_at(3), "V4")]
    corroboration_too_late = behind + [(_at(86400), "V5"), (_at(86400), "V6")]
    assert guard.decide(_at(86400), "V1", corroboration_too_late, complete=False) is True
    assert GUARD.decide(_at(86400), "V1", corroboration_too_late, complete=True) is False


def test_decision_rule_decides_hold_early_when_corroboration_is_impossible():
    guard = WatermarkGuard.from_config(_config(lookahead_events=3).watermark_guard)
    assert guard.decide(_at(86400), "V1", [(_at(1), "V2"), (_at(2), "V3")], complete=False) is True


def test_is_jump():
    assert GUARD.is_jump(T0, None)
    assert not GUARD.is_jump(_at(900), T0)
    assert GUARD.is_jump(_at(900.000001), T0)


def _sequence(events, guard=GUARD):
    """events: [(kind, id, event_time, vehicle)] -> released [(id, status, held)] in order."""
    out = []
    tracker = EventTimeTracker(allowed_lateness_seconds=60)
    seq = EventTimeSequencer(
        tracker, guard,
        emit_other=lambda item: out.append((item, None, None)),
        emit_event=lambda item, d: out.append((item, d.status, d.held_back)),
        emit_probe=lambda item, held: out.append((item, "probe", held)),
    )
    for kind, item, when, vehicle in events:
        getattr(seq, kind)(item, when, vehicle) if kind != "other" else seq.other(item)
    seq.finish()
    return out, tracker, seq


def test_sequencer_releases_in_arrival_order_and_holds_back_the_outlier():
    fleet = [("event", f"e{i}", _at(i), f"V{i % 5}") for i in range(30)]
    events = fleet[:3] + [("event", "OUT", _at(86400), "V9"), ("other", "dup", None, None),
                          ("probe", "Q", _at(86400), "V8")] + fleet[3:]
    out, tracker, seq = _sequence(events)
    assert [item for item, _, _ in out] == [e[1] for e in events]
    released = {item: (status, held) for item, status, held in out}
    assert released["OUT"] == (IN_ORDER, True)
    assert released["Q"] == ("probe", True)
    assert all(released[f"e{i}"] == (IN_ORDER, False) for i in range(30))
    assert tracker.max_event_time == _at(29)
    assert (seq.held_back, seq.probes_held_back) == (1, 1)
    assert seq.max_buffered <= GUARD.lookahead_events + 3


def test_sequencer_follows_a_corroborated_jump_exactly_as_without_the_guard():
    """A genuine jump followed by a straggler from before the jump and then
    the rest of the fleet: flags are those of the unguarded watermark."""
    before = [("event", f"a{i}", _at(i), f"V{i % 5}") for i in range(10)]
    after = [("event", f"b{i}", _at(86400 + i), f"V{i % 5}") for i in range(10)]
    events = before + after[:1] + [("event", "straggler", _at(9.5), "V7")] + after[1:]
    off = WatermarkGuard.from_config(_config(mode="off").watermark_guard)
    assert off.enabled is False
    guarded, _, seq = _sequence(events)
    unguarded, _, _ = _sequence(events, guard=off)
    assert guarded == unguarded
    assert {i: s for i, s, _ in guarded}["straggler"] == LATE
    assert seq.held_back == 0


def test_tracker_can_classify_without_advancing():
    tracker = EventTimeTracker(allowed_lateness_seconds=60)
    tracker.observe(T0)
    assert tracker.observe(T0 + timedelta(days=365 * 60), advance=False)[0] == IN_ORDER
    assert tracker.max_event_time == T0
    assert tracker.observe(T0 + timedelta(seconds=1))[0] == IN_ORDER  # not late


def test_outlier_is_inside_the_fu10_bounds():
    """The reproduction is meaningful only if the absolute bounds do not
    already reject it (FU-10 removes absurd timestamps; FU-06 is about
    plausible-but-wrong ones)."""
    outlier = datetime.fromisoformat(OUTLIER_TIME)
    assert THRESHOLDS.min_timestamp <= outlier <= THRESHOLDS.max_timestamp


# -- opt-in clock-relative rule: arrival_time_skew -----------------------------------------

SKEW_ON = {"mode": "quarantine"}


def _iso(base: datetime, seconds: float) -> str:
    return (base + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def test_arrival_time_skew_is_off_by_default(tmp_path):
    when = _iso(FIXED_TIME, 86400 * 365)  # a year after Bronze ingestion
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload(event_time=when, generated_at=when))])
    assert result.manifest["counters"]["valid_loaded"] == 1
    assert result.manifest["counters"]["rule_violations"]["arrival_time_skew"] == 0


@pytest.mark.parametrize(
    ("seconds_after_ingestion", "quarantined"),
    [(0, False), (300, False), (300.000001, True), (3600, True), (-86400, False)],
)
def test_arrival_time_skew_boundary_against_bronze_ingestion_time(tmp_path, seconds_after_ingestion, quarantined):
    # Bronze ingestion_time in the helpers is FIXED_TIME; default max_skew_seconds = 300.
    assert SILVER_CONFIG.arrival_time_skew.max_skew_seconds == 300
    when = _iso(FIXED_TIME, seconds_after_ingestion)
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload(event_time=when, generated_at=when))],
                             config=_config(arrival_time_skew=SKEW_ON))
    counters = result.manifest["counters"]
    assert counters["quarantined_by_rule"]["arrival_time_skew"] == int(quarantined)
    assert counters["valid_loaded"] == int(not quarantined)


def test_arrival_time_skew_reference_is_configurable(tmp_path):
    # 20 minutes after ingestion (09:30), but 10 minutes before processing (10:00).
    when = _iso(FIXED_TIME, 1200)
    assert FIXED_TIME < datetime.fromisoformat(when) < PROCESSING_TIME
    line = json.dumps(valid_payload(event_time=when, generated_at=when))
    by_ingestion = lines_to_silver(tmp_path / "i", [line], config=_config(arrival_time_skew=SKEW_ON))
    assert by_ingestion.manifest["counters"]["quarantined_by_rule"]["arrival_time_skew"] == 1
    (record,) = table(by_ingestion, "quarantine")
    assert "ingestion_time" in record["rejections"][0]["reason"]
    by_processing = lines_to_silver(tmp_path / "p", [line], config=_config(
        arrival_time_skew={**SKEW_ON, "reference": "processing_time"}))
    assert by_processing.manifest["counters"]["valid_loaded"] == 1
    wider = lines_to_silver(tmp_path / "w", [line], config=_config(
        arrival_time_skew={**SKEW_ON, "max_skew_seconds": 1200}))
    assert wider.manifest["counters"]["valid_loaded"] == 1


def test_each_delivery_of_an_arrival_skew_reject_is_quarantined(tmp_path):
    line = _outlier_line()
    result = lines_to_silver(tmp_path, [json.dumps(clean_payloads(1)[0]), line, line],
                             config=_config(arrival_time_skew=SKEW_ON))
    counters = result.manifest["counters"]
    assert counters["quarantined_by_rule"]["arrival_time_skew"] == 2
    assert counters["duplicates_excluded"] == 0 and counters["valid_loaded"] == 1
