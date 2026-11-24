"""DE-07 end-to-end: an incident-laced stream with ALL DE-03 scenarios
enabled -> DE-05 Bronze -> DE-06 Silver -> DE-07 Gold, through the four
real CLIs as separate processes, with Gold's reconciliation to Silver
checked against counts taken independently from the Silver tables."""

from __future__ import annotations

import json
import subprocess
import sys
from collections import Counter

from simulator.tests._bronze_helpers import FAST_GENERATOR_CONFIG, REPO_ROOT, read_jsonl
from simulator.tests._gold_helpers import store_files

SCENARIO_FLAGS = [
    f"--enable-{s}"
    for s in ("malformed", "duplicate", "late-event", "out-of-order", "congestion", "volume-spike", "breakdown")
]


def _run(*args: str) -> dict:
    done = subprocess.run([sys.executable, "-m", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True)
    return json.loads(done.stdout.strip().splitlines()[-1]) if done.stdout.strip() else {}


def _gold_cli(tmp_path, out_name: str) -> dict:
    return _run("simulator.gold", "--run-id", "g1", "--silver-dir", str(tmp_path / "silver"),
                "--output-dir", str(tmp_path / out_name), "--fixed-processing-time", "2025-02-01T11:00:00Z",
                "--log-file", str(tmp_path / f"{out_name}.log"))


def test_all_scenarios_through_bronze_silver_and_gold_clis(tmp_path):
    events, incident_log = tmp_path / "events.ndjson", tmp_path / "incidents.log"
    _run("simulator.incidents", "--generator-config", str(FAST_GENERATOR_CONFIG), "--max-events", "600",
         "--output", str(events), "--log-file", str(incident_log), *SCENARIO_FLAGS)
    fired = Counter(
        rec["context"]["scenario"] for rec in map(json.loads, incident_log.read_text().splitlines())
        if rec["message"].endswith("incident fired")
    )
    for scenario in ("malformed", "duplicate", "late_event", "out_of_order", "congestion", "volume_spike",
                     "breakdown"):
        assert fired[scenario] > 0, scenario

    _run("simulator.bronze", "stream", "--input", str(events), "--run-id", "b1", "--output-dir",
         str(tmp_path / "bronze"), "--fixed-ingestion-time", "2025-02-01T09:30:00Z")
    silver_summary = _run("simulator.silver", "--run-id", "s1", "--bronze-dir", str(tmp_path / "bronze"),
                          "--output-dir", str(tmp_path / "silver"), "--fixed-processing-time",
                          "2025-02-01T10:00:00Z")
    assert silver_summary["status"] == "COMMITTED" and silver_summary["reconciliation_holds"] is True
    summary = _gold_cli(tmp_path, "gold")
    assert summary["status"] == "COMMITTED" and summary["reconciliation_holds"] is True
    assert summary["silver_runs_consumed"] == ["s1"]

    silver_manifest = json.loads((tmp_path / "silver" / "_runs" / "s1.json").read_text())
    s = silver_manifest["counters"]
    clean = read_jsonl(tmp_path / "silver" / silver_manifest["tables"]["clean"]["path"])
    quarantine = read_jsonl(tmp_path / "silver" / silver_manifest["tables"]["quarantine"]["path"])
    gold_manifest = json.loads((tmp_path / "gold" / "_runs" / "g1.json").read_text())
    rows = read_jsonl(tmp_path / "gold" / gold_manifest["tables"]["route_performance"]["path"])
    c = gold_manifest["counters"]

    # The scenarios really reached every Silver bucket and flag.
    assert s["quarantined"] > 0 and s["duplicates_excluded"] > 0 and s["late"] > 0 and s["out_of_order"] > 0
    # Reconciliation identities, against counts taken from the Silver tables.
    assert sum(r["event_count"] for r in rows) == len(clean) == s["valid_loaded"]
    assert c["quarantined_attributed"] + c["quarantined_unattributed"] == len(quarantine) == s["quarantined"]
    assert sum(r["quarantined_count"] for r in rows) == c["quarantined_attributed"]
    assert sum(r["late_event_count"] for r in rows) == s["late"]
    assert sum(r["out_of_order_event_count"] for r in rows) == s["out_of_order"]
    assert all(gold_manifest["reconciliation"]["checks"].values())
    # Unknown-route records can never be attributed to a route.
    assert c["unattributed_by_reason"]["route_id_missing_or_unknown"] >= s["quarantined_by_rule"]["known_route"] > 0

    # Per (route, UTC hour) counts and delay sums, computed independently from
    # the clean table (hourly buckets at a whole-hour offset == UTC hours).
    expected_count = Counter((r["route_id"], r["event_time"][:13]) for r in clean)
    expected_delay = Counter()
    for r in clean:
        expected_delay[(r["route_id"], r["event_time"][:13])] += r["delay_seconds"]
    got = {(r["route_id"], r["period_start"][:13]): r for r in rows if r["event_count"]}
    assert {k: v["event_count"] for k, v in got.items()} == dict(expected_count)
    assert {k: v["sum_delay_seconds"] for k, v in got.items()} == dict(expected_delay)
    for row in rows:
        if row["event_count"]:
            assert row["avg_delay_seconds"] == round(row["sum_delay_seconds"] / row["event_count"], 4)
        assert 0.0 <= row["valid_percentage"] <= 100.0
    # Congestion (+240 s on ROUTE-01) shows up: ROUTE-01 has the highest overall average delay.
    per_route = Counter()
    per_route_n = Counter()
    for r in rows:
        per_route[r["route_id"]] += r["sum_delay_seconds"]
        per_route_n[r["route_id"]] += r["event_count"]
    averages = {route: per_route[route] / per_route_n[route] for route in per_route if per_route_n[route]}
    assert max(averages, key=averages.get) == "ROUTE-01"

    # Deterministic: a second Gold process over the same Silver run is byte-identical.
    _gold_cli(tmp_path, "gold_again")
    assert store_files(tmp_path / "gold") == store_files(tmp_path / "gold_again")
