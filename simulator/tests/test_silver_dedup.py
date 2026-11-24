"""DE-06 deduplication by event_id (validate-then-dedup, first valid arrival
wins), within a Bronze run and across Bronze runs - including a deliberate
Bronze `--allow-replay` run, which must not double-count anything."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from simulator.bronze import fixed_clock
from simulator.tests._silver_helpers import (
    bronze_from_lines,
    clean_payloads,
    lines_to_silver,
    silver,
    table,
    valid_payload,
)

LATER = fixed_clock(datetime(2025, 2, 2, tzinfo=UTC))


def test_duplicate_within_a_run_is_excluded_counted_and_audited(tmp_path):
    p = clean_payloads(3)
    lines = [json.dumps(p[0]), json.dumps(p[1]), json.dumps(p[0]), json.dumps(p[2]), json.dumps(p[0])]
    result = lines_to_silver(tmp_path, lines)
    counters = result.manifest["counters"]
    assert (counters["extracted"], counters["valid_loaded"], counters["duplicates_excluded"]) == (5, 3, 2)
    assert counters["duplicate_payload_conflicts"] == 0
    clean = table(result, "clean")
    assert [r["event_id"] for r in clean] == [p[0]["event_id"], p[1]["event_id"], p[2]["event_id"]]
    assert clean[0]["_bronze_line_number"] == 1  # first arrival wins
    dups = table(result, "duplicates")
    assert [d["_bronze_line_number"] for d in dups] == [3, 5]
    assert all(d["event_id"] == p[0]["event_id"] and d["payload_matches_kept"] for d in dups)
    assert all(d["kept"]["_bronze_line_number"] == 1 for d in dups)


def test_bronze_replay_run_does_not_double_count(tmp_path):
    lines = [json.dumps(p) for p in clean_payloads(40)] + ["garbage"]
    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, lines, run_id="b1", name="events.ndjson")
    single = silver(bronze, tmp_path / "silver_single", run_id="single")
    # Deliberate replay of the same file into a new Bronze run (DE-05 --allow-replay).
    bronze_from_lines(bronze, lines, run_id="b2", name="events.ndjson", clock=LATER, allow_replay=True)
    both = silver(bronze, tmp_path / "silver_both", run_id="both")
    s, b = single.manifest["counters"], both.manifest["counters"]
    assert [r["run_id"] for r in both.manifest["bronze_runs_consumed"]] == ["b1", "b2"]
    assert both.manifest["bronze_runs_consumed"][1]["replay_of"] == ["b1"]
    assert b["extracted"] == 2 * s["extracted"] == 82
    assert b["valid_loaded"] == s["valid_loaded"] == 40
    assert b["duplicates_excluded"] == 40
    # Invalid deliveries are each quarantined (validate-then-dedup): the replayed garbage line too.
    assert b["quarantined"] == 2 * s["quarantined"] == 2
    # The clean table is identical apart from nothing: same events, same (first) lineage.
    strip = lambda rows: [{k: v for k, v in r.items() if k != "silver_run_id"} for r in rows]
    assert strip(table(both, "clean")) == strip(table(single, "clean"))
    assert {d["bronze_run_id"] for d in table(both, "duplicates")} == {"b2"}


def test_cross_run_order_is_bronze_commit_order_not_the_order_requested(tmp_path):
    p = clean_payloads(1)[0]
    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, [json.dumps(p)], run_id="zz-first")
    bronze_from_lines(bronze, [json.dumps({**p, "occupancy": 7})], run_id="aa-second", clock=LATER)
    result = silver(bronze, bronze_run_ids=["aa-second", "zz-first"])
    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["zz-first", "aa-second"]
    (kept,) = table(result, "clean")
    assert kept["bronze_run_id"] == "zz-first"
    (dup,) = table(result, "duplicates")
    assert dup["bronze_run_id"] == "aa-second"


def test_same_event_id_with_different_payload_is_a_counted_conflict(tmp_path):
    p = clean_payloads(1)[0]
    result = lines_to_silver(tmp_path, [json.dumps(p), json.dumps({**p, "occupancy": p["occupancy"] + 1})])
    counters = result.manifest["counters"]
    assert counters["duplicates_excluded"] == 1 and counters["duplicate_payload_conflicts"] == 1
    (dup,) = table(result, "duplicates")
    assert dup["payload_matches_kept"] is False
    assert table(result, "clean")[0]["occupancy"] == p["occupancy"]


def test_invalid_copy_does_not_claim_the_event_id(tmp_path):
    # validate-then-dedup: a corrupt first copy is quarantined and the later
    # good copy is loaded, not discarded as a duplicate.
    p = clean_payloads(1)[0]
    result = lines_to_silver(tmp_path, [json.dumps({**p, "speed_kmh": -42.0}), json.dumps(p)])
    counters = result.manifest["counters"]
    assert (counters["quarantined"], counters["valid_loaded"], counters["duplicates_excluded"]) == (1, 1, 0)


def test_repeated_invalid_deliveries_are_each_quarantined(tmp_path):
    bad = json.dumps(valid_payload(vehicle_id=None))
    counters = lines_to_silver(tmp_path, [bad, bad, bad]).manifest["counters"]
    assert (counters["quarantined"], counters["duplicates_excluded"], counters["valid_loaded"]) == (3, 0, 0)


def test_dedup_key_is_exact_event_id(tmp_path):
    p = clean_payloads(1)[0]
    variant = {**p, "event_id": p["event_id"].lower()}
    counters = lines_to_silver(tmp_path, [json.dumps(p), json.dumps(variant)]).manifest["counters"]
    assert counters["valid_loaded"] == 2 and counters["duplicates_excluded"] == 0
