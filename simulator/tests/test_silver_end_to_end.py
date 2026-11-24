"""DE-06 end-to-end: an incident-laced stream with ALL DE-03 scenarios
enabled -> DE-05 Bronze -> DE-06 Silver, with Silver's counters asserted
against counts derived independently from the DE-03 structured incident log
(see `_silver_helpers.derive_expectations`; no Silver code is used to build
the expectation).

Two variants: in-process (large, plus a Bronze replay run and a run of
unparseable lines) and the three real CLIs as separate processes."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime

from simulator.bronze import fixed_clock
from simulator.generator.config import load_generator_config
from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    IncidentsConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
    VolumeSpikeConfig,
)
from simulator.incidents.malformed import MALFORMED_MODES
from simulator.tests._bronze_helpers import FAST_GENERATOR_CONFIG, REPO_ROOT
from simulator.tests._generator_helpers import build_generator
from simulator.tests._silver_helpers import (
    MODE_RULES,
    bronze_from_bytes,
    bronze_from_lines,
    delivered_event_ids,
    derive_expectations,
    fired,
    parse_incident_log,
    run_incident_stream,
    silver,
    table,
)

ALL_SCENARIOS = IncidentsConfig(
    seed=424242,
    malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.08),
    duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.08),
    late_event=LateEventConfig(enabled=True, probability_per_tick=0.06),
    out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.3, hold_ticks=3),
    congestion=CongestionConfig(enabled=True, route_id="ROUTE-01",
                                window=RecoveryWindow(start_tick=100, duration_ticks=600)),
    volume_spike=VolumeSpikeConfig(enabled=True, rate_multiplier=6.0,
                                   window=RecoveryWindow(start_tick=300, duration_ticks=300)),
    breakdown=BreakdownConfig(enabled=True, vehicle_ids=["VEH-0001", "VEH-0007"],
                              window=RecoveryWindow(start_tick=0, duration_ticks=900)),
)
DAY2 = fixed_clock(datetime(2025, 2, 2, tzinfo=UTC))
DAY3 = fixed_clock(datetime(2025, 2, 3, tzinfo=UTC))
GARBAGE = b'{"event_id": "EVT-TRUNC", "speed_kmh": 1\n\xff\xfe\x00\n[1,2]\n'


def _assert_counters_match(counters, expected):
    assert counters["extracted"] == expected.extracted
    assert counters["valid_loaded"] == expected.valid_loaded
    assert counters["quarantined"] - counters["parse_errors"] == expected.quarantined
    assert counters["duplicates_excluded"] == expected.duplicates_excluded
    assert counters["late"] == expected.late
    assert counters["out_of_order"] == expected.out_of_order
    assert counters["in_order"] == expected.in_order
    by_rule = {k: v for k, v in counters["quarantined_by_rule"].items() if v and k != "bronze_parse_error"}
    assert by_rule == dict(expected.quarantined_by_rule)


def test_all_scenarios_through_bronze_and_silver_match_the_incident_log(tmp_path):
    run = run_incident_stream(ALL_SCENARIOS, max_events=2000)
    # Guard: every scenario really fired.
    for scenario in ("malformed", "duplicate", "late_event", "out_of_order", "congestion", "volume_spike",
                     "breakdown"):
        assert fired(run.log, scenario), scenario
    assert {c["mode"] for c in fired(run.log, "malformed")} == set(MALFORMED_MODES)

    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, run.lines, run_id="b1", name="events.ndjson")
    # A deliberate DE-05 replay of the same file, then a run of unparseable lines.
    bronze_from_lines(bronze, run.lines, run_id="b2", name="events.ndjson", allow_replay=True,
                      clock=DAY2)
    bronze_from_bytes(bronze, GARBAGE, run_id="b3", clock=DAY3)

    result = silver(bronze)
    counters = result.manifest["counters"]
    ids = delivered_event_ids(run.lines)
    expected = derive_expectations(run.log, ids + ids, run.total_ticks)

    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["b1", "b2", "b3"]
    assert counters["extracted"] == 2 * len(run.lines) + 3
    assert counters["parse_errors"] == 3
    assert counters["parse_errors_by_code"]["invalid_json"] == 1
    assert counters["parse_errors_by_code"]["invalid_utf8"] == 1
    assert counters["parse_errors_by_code"]["not_a_json_object"] == 1
    expected.extracted += 3
    _assert_counters_match(counters, expected)
    # The replay doubled nothing in the clean table.
    single = derive_expectations(run.log, ids, run.total_ticks)
    assert counters["valid_loaded"] == single.valid_loaded
    assert {r["event_id"] for r in table(result, "clean")} == single.valid_ids
    # Every malformed mode reached quarantine under its rule.
    for mode, rule in MODE_RULES.items():
        assert counters["quarantined_by_rule"][rule] > 0, mode
    # Late: exactly the logged late injections that were delivered intact.
    clean = table(result, "clean")
    late_ids = {r["event_id"] for r in clean if r["_silver_event_time_status"] == "late"}
    assert late_ids == expected.late_ids
    malformed_ticks = {c["tick"] for c in fired(run.log, "malformed")}
    late_ticks = {c["tick"] for c in fired(run.log, "late_event")}
    logged_late = {
        event_id for event_id, tick in expected.tick_of.items() if tick in late_ticks - malformed_ticks
    }
    assert late_ids == logged_late & set(ids)
    # Congestion and breakdown stayed valid (not misclassified as quarantine).
    assert any(r["status"] == "BREAKDOWN" for r in clean)
    baseline = build_generator()
    base = {e.event_id: e for e in (baseline.produce_event(t) for t in range(run.total_ticks))}
    congested = [r for r in clean if r["route_id"] == "ROUTE-01"
                 and r["delay_seconds"] == base[r["event_id"]].delay_seconds + 240]
    assert congested
    assert result.manifest["reconciliation"]["holds"]


def test_real_cli_pipeline_matches_the_incident_log(tmp_path):
    events, log_file = tmp_path / "events.ndjson", tmp_path / "incidents.log"
    flags = [f"--enable-{s}" for s in ("malformed", "duplicate", "late-event", "out-of-order", "congestion",
                                        "volume-spike", "breakdown")]
    subprocess.run(
        [sys.executable, "-m", "simulator.incidents", "--generator-config", str(FAST_GENERATOR_CONFIG),
         "--max-events", "500", "--output", str(events), "--log-file", str(log_file), *flags],
        cwd=REPO_ROOT, check=True, capture_output=True,
    )
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    subprocess.run(
        [sys.executable, "-m", "simulator.bronze", "stream", "--input", str(events), "--run-id", "b1",
         "--output-dir", str(bronze), "--fixed-ingestion-time", "2025-02-01T09:30:00Z"],
        cwd=REPO_ROOT, check=True, capture_output=True,
    )
    done = subprocess.run(
        [sys.executable, "-m", "simulator.silver", "--run-id", "s1", "--bronze-dir", str(bronze),
         "--output-dir", str(out), "--fixed-processing-time", "2025-02-01T10:00:00Z"],
        cwd=REPO_ROOT, check=True, capture_output=True,
    )
    summary = json.loads(done.stdout)
    assert summary["status"] == "COMMITTED" and summary["reconciliation_holds"] is True

    log = parse_incident_log(log_file.read_text())
    lines = events.read_text().splitlines()
    expected = derive_expectations(
        log, delivered_event_ids(lines), 500 + 3, generator_config=load_generator_config(FAST_GENERATOR_CONFIG)
    )
    assert fired(log, "malformed") and fired(log, "duplicate") and fired(log, "late_event")
    manifest = json.loads((out / "_runs" / "s1.json").read_text())
    _assert_counters_match(manifest["counters"], expected)
    assert summary["extracted"] == len(lines)
    assert summary["valid_loaded"] + summary["quarantined"] + summary["duplicates_excluded"] == len(lines)
