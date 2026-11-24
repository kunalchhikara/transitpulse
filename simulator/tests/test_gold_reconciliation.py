"""DE-07 reconciliation: the Gold aggregate reconciles exactly to the Silver
run it consumed (asserted in code before commit), lineage is recorded, a
broken aggregate fails loudly with nothing committed, and FU-07 replay
sensitivity is flagged."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

import simulator.gold.pipeline as gold_pipeline
from simulator.bronze import fixed_clock
from simulator.gold import RECONCILIATION_IDENTITIES, GoldAccountingError
from simulator.tests._gold_helpers import (
    EXPECTED_TOTALS,
    fixture_lines,
    fixture_silver,
    gold,
    gold_rows,
    silver_table,
)
from simulator.tests._silver_helpers import bronze_from_lines, silver


def test_reconciliation_identity_holds_against_independent_counts(tmp_path):
    silver_result = fixture_silver(tmp_path)
    result = gold(tmp_path)
    manifest, rows = result.manifest, gold_rows(result)
    recon = manifest["reconciliation"]
    assert recon["holds"] is True and all(recon["checks"].values())
    assert recon["identities"] == list(RECONCILIATION_IDENTITIES)

    s = silver_result.manifest["counters"]
    clean, quarantine = silver_table(silver_result, "clean"), silver_table(silver_result, "quarantine")
    # Independently: count the Silver tables ourselves.
    assert sum(r["event_count"] for r in rows) == len(clean) == s["valid_loaded"]
    c = manifest["counters"]
    assert c["quarantined_attributed"] + c["quarantined_unattributed"] == len(quarantine) == s["quarantined"]
    assert sum(r["quarantined_count"] for r in rows) == c["quarantined_attributed"]
    assert sum(r["late_event_count"] for r in rows) == s["late"] == sum(
        1 for r in clean if r["_silver_event_time_status"] == "late"
    )
    assert sum(r["sum_delay_seconds"] for r in rows) == sum(r["delay_seconds"] for r in clean)
    assert c["overall_valid_percentage"] == s["quality_percentage"] == EXPECTED_TOTALS["overall_valid_percentage"]
    assert "8 event_count == 8 silver valid_loaded" in recon["formula"]


def test_manifest_records_lineage(tmp_path):
    silver_result = fixture_silver(tmp_path)
    manifest = gold(tmp_path).manifest
    assert manifest["layer"] == "gold" and manifest["status"] == "COMMITTED"
    assert manifest["silver_selection"] == "latest_committed"
    (consumed,) = manifest["silver_runs_consumed"]
    assert consumed["run_id"] == "s1"
    assert consumed["bronze_runs"] == ["b1"]
    assert consumed["input_fingerprint"] == silver_result.manifest["input_fingerprint"]
    for role in ("clean", "quarantine", "duplicates"):
        assert consumed["tables"][role]["sha256"] == silver_result.manifest["tables"][role]["sha256"]
    assert consumed["reference_sha256"] == manifest["reference_sha256"]
    assert consumed["silver_to_gold_latency_seconds"] == 3600.0  # 11:00 Gold - 10:00 Silver
    assert manifest["aggregate_definition"]["event_count"].startswith("Silver clean records")
    table = manifest["tables"]["route_performance"]
    assert table["path"] == "route_performance/run_id=g1/part-00000.jsonl"
    assert table["records"] == 6


def test_every_row_carries_run_lineage(tmp_path):
    fixture_silver(tmp_path)
    for row in gold_rows(gold(tmp_path)):
        assert (row["gold_run_id"], row["silver_run_id"]) == ("g1", "s1")


def test_a_row_lost_in_aggregation_fails_loudly_and_commits_nothing(tmp_path, monkeypatch):
    fixture_silver(tmp_path)
    real = gold_pipeline.aggregate_route_performance

    def lossy(*args, **kwargs):
        agg = real(*args, **kwargs)
        agg.rows.pop(0)
        return agg

    monkeypatch.setattr(gold_pipeline, "aggregate_route_performance", lossy)
    with pytest.raises(GoldAccountingError, match="event_count_equals_clean_read"):
        gold(tmp_path)
    manifest_path = tmp_path / "gold" / "_runs" / "g1.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["status"] == "FAILED"
    assert manifest["error"]["type"] == "GoldAccountingError"
    assert manifest["reconciliation"]["holds"] is False
    assert not (tmp_path / "gold" / "route_performance" / "run_id=g1").exists()
    assert not (tmp_path / "gold" / "_staging").exists()


def test_bronze_replay_in_lineage_is_flagged_and_still_reconciles(tmp_path):
    """FU-07: a DE-05 --allow-replay run re-quarantines the same rejected
    records, inflating `quarantined`. Gold does not hide that: it flags the
    run as replay-sensitive; the replayed valid events are duplicates and
    are never double-counted."""
    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, fixture_lines(), run_id="b1", name="events.ndjson")
    bronze_from_lines(bronze, fixture_lines(), run_id="b2", name="events.ndjson", allow_replay=True,
                      clock=fixed_clock(datetime(2025, 2, 1, 9, 45, tzinfo=UTC)))
    silver(bronze, tmp_path / "silver")
    result = gold(tmp_path)
    c = result.manifest["counters"]
    assert c["replay_sensitive"] is True
    assert result.manifest["silver_runs_consumed"][0]["replayed_bronze_runs"] == ["b2"]
    assert c["event_count"] == 8  # not 16: replayed valid events are excluded duplicates
    assert c["quarantined_attributed"] + c["quarantined_unattributed"] == 16  # inflated, as FU-07 describes
    assert result.manifest["reconciliation"]["holds"] is True
