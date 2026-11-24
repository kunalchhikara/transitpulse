"""DE-06 acceptance: each DE-03 incident scenario, enabled on its own, has
the expected Silver outcome. Expectations come from the DE-03 structured
incident log and a clean DE-02 generator, never from Silver's own logic."""

from __future__ import annotations

from collections import Counter

import pytest

from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
    VolumeSpikeConfig,
)
from simulator.incidents.malformed import MALFORMED_MODES
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import baseline_incidents_config
from simulator.tests._silver_helpers import (
    MODE_RULES,
    delivered_event_ids,
    derive_expectations,
    fired,
    lines_to_silver,
    run_incident_stream,
    table,
)


def _silver_for(tmp_path, max_events, **scenarios):
    incident_run = run_incident_stream(baseline_incidents_config(**scenarios), max_events)
    result = lines_to_silver(tmp_path, incident_run.lines)
    expected = derive_expectations(
        incident_run.log, delivered_event_ids(incident_run.lines), incident_run.total_ticks
    )
    return incident_run, result, expected


def _assert_matches_oracle(counters, expected):
    assert counters["extracted"] == expected.extracted
    assert counters["valid_loaded"] == expected.valid_loaded
    assert counters["quarantined"] == expected.quarantined
    assert counters["duplicates_excluded"] == expected.duplicates_excluded
    assert counters["late"] == expected.late
    assert counters["out_of_order"] == expected.out_of_order
    assert counters["in_order"] == expected.in_order
    assert {k: v for k, v in counters["quarantined_by_rule"].items() if v} == dict(expected.quarantined_by_rule)


def _baseline_by_tick(total_ticks):
    generator = build_generator()
    return {tick: generator.produce_event(tick) for tick in range(total_ticks)}


@pytest.mark.parametrize("mode", MALFORMED_MODES)
def test_each_malformed_mode_is_quarantined_with_the_correct_rule(tmp_path, mode):
    run, result, expected = _silver_for(
        tmp_path, 30, malformed=MalformedDataConfig(enabled=True, probability_per_tick=1.0, modes=[mode])
    )
    assert len(fired(run.log, "malformed")) == 30
    counters = result.manifest["counters"]
    assert counters["quarantined"] == 30 and counters["valid_loaded"] == 0
    assert counters["quarantined_by_rule"][MODE_RULES[mode]] == 30
    assert {r["primary_rule"] for r in table(result, "quarantine")} == {MODE_RULES[mode]}
    _assert_matches_oracle(counters, expected)


def test_duplicate_injection_is_excluded_and_never_double_counted(tmp_path):
    run, result, expected = _silver_for(
        tmp_path, 300, duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.3)
    )
    replays = fired(run.log, "duplicate")
    assert replays
    counters = result.manifest["counters"]
    assert 0 < counters["duplicates_excluded"] <= len(replays)
    _assert_matches_oracle(counters, expected)
    clean_ids = [r["event_id"] for r in table(result, "clean")]
    assert len(clean_ids) == len(set(clean_ids)) == len(set(delivered_event_ids(run.lines)))
    assert Counter(d["event_id"] for d in table(result, "duplicates")) <= Counter(r["replayed_event_id"] for r in replays)


def test_late_events_are_loaded_flagged_late_per_the_watermark(tmp_path):
    run, result, expected = _silver_for(
        tmp_path, 300, late_event=LateEventConfig(enabled=True, probability_per_tick=0.2)
    )
    injected = fired(run.log, "late_event")
    assert injected
    baseline = _baseline_by_tick(run.total_ticks)
    skew_by_id = {baseline[c["tick"]].event_id: c["skew_seconds"] for c in injected}
    clean = table(result, "clean")
    late = {r["event_id"]: r for r in clean if r["_silver_event_time_status"] == "late"}
    # Straight from the log: exactly the injected events are late (skew >= 120 s > 60 s lateness).
    assert set(late) == set(skew_by_id)
    for event_id, record in late.items():
        skew = skew_by_id[event_id]
        assert skew - 1.0 < record["_silver_lateness_seconds"] <= skew - 0.1 + 1e-9
    counters = result.manifest["counters"]
    assert counters["late"] == len(injected) and counters["quarantined"] == 0
    assert counters["valid_loaded"] == 300
    _assert_matches_oracle(counters, expected)


def test_out_of_order_delivery_is_accepted_and_flagged(tmp_path):
    run, result, expected = _silver_for(
        tmp_path, 300, out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.5, hold_ticks=3)
    )
    assert fired(run.log, "out_of_order")
    counters = result.manifest["counters"]
    assert counters["valid_loaded"] == counters["extracted"] == run.total_ticks == 303  # + reorder-buffer prefill
    assert counters["quarantined"] == counters["duplicates_excluded"] == counters["late"] == 0
    assert counters["out_of_order"] > 0
    _assert_matches_oracle(counters, expected)
    # Every flagged event really was overtaken by a later-generated event.
    tick_of = expected.tick_of
    max_tick = -1
    for record in table(result, "clean"):
        tick = tick_of[record["event_id"]]
        assert (record["_silver_event_time_status"] == "out_of_order") == (tick < max_tick)
        max_tick = max(max_tick, tick)


def test_congestion_events_are_valid_and_keep_their_degraded_values(tmp_path):
    route_id = build_generator().produce_event(0).route_id
    window = RecoveryWindow(start_tick=50, duration_ticks=100)
    _, result, _ = _silver_for(
        tmp_path, 250, congestion=CongestionConfig(enabled=True, route_id=route_id, window=window)
    )
    counters = result.manifest["counters"]
    assert counters["valid_loaded"] == 250 and counters["quarantined"] == 0
    baseline = _baseline_by_tick(250)
    congested = recovered = 0
    for tick, record in enumerate(table(result, "clean")):
        base = baseline[tick]
        assert record["event_id"] == base.event_id
        if record["route_id"] == route_id and window.contains(tick):
            congested += 1
            assert record["speed_kmh"] == round(base.speed_kmh * 0.3, 2)
            assert record["delay_seconds"] == base.delay_seconds + 240
        else:
            recovered += tick >= window.start_tick + window.duration_ticks and record["route_id"] == route_id
            assert record["speed_kmh"] == base.speed_kmh and record["delay_seconds"] == base.delay_seconds
    assert congested > 0 and recovered > 0


def test_volume_spike_events_are_all_valid(tmp_path):
    run, result, expected = _silver_for(
        tmp_path, 200,
        volume_spike=VolumeSpikeConfig(enabled=True, rate_multiplier=8.0,
                                       window=RecoveryWindow(start_tick=20, duration_ticks=100)),
    )
    messages = [r["message"] for r in run.log]
    assert "volume_spike incident fired" in messages and "volume_spike incident recovered" in messages
    counters = result.manifest["counters"]
    assert counters["valid_loaded"] == counters["extracted"] == 200
    assert counters["quarantined"] == counters["duplicates_excluded"] == counters["late"] == 0
    _assert_matches_oracle(counters, expected)


def test_breakdown_events_are_valid_and_recover(tmp_path):
    vehicle_id = build_generator().produce_event(0).vehicle_id
    window = RecoveryWindow(start_tick=0, duration_ticks=150)
    _, result, _ = _silver_for(
        tmp_path, 300, breakdown=BreakdownConfig(enabled=True, vehicle_ids=[vehicle_id], window=window)
    )
    counters = result.manifest["counters"]
    assert counters["valid_loaded"] == 300 and counters["quarantined"] == 0
    mine = [(t, r) for t, r in enumerate(table(result, "clean")) if r["vehicle_id"] == vehicle_id]
    during = [r for t, r in mine if window.contains(t)]
    after = [r for t, r in mine if not window.contains(t)]
    assert during and after
    assert all(r["status"] == "BREAKDOWN" and r["speed_kmh"] == 0.0 for r in during)
    assert all(r["status"] != "BREAKDOWN" for r in after)
    assert all(r["ref_vehicle_home_depot_id"] for _, r in mine)
