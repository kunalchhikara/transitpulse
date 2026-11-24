"""DE-07 Silver read contract (BACKLOG DE-07 "Silver input contract"): Gold
reads Silver only through committed Silver run manifests, verifies every
data file against its manifest, and never reads uncommitted or tampered
Silver output."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest

from simulator.bronze import fixed_clock
from simulator.config import load_scale_config
from simulator.gold import GoldInputError
from simulator.tests._gold_helpers import (
    EXPECTED_ROWS,
    fixture_silver,
    gold,
    gold_rows,
)
from simulator.tests._silver_helpers import silver


def _gold_committed(tmp_path) -> bool:
    path = tmp_path / "gold" / "_runs" / "g1.json"
    return path.exists() and json.loads(path.read_text())["status"] == "COMMITTED"


def test_no_committed_silver_run_is_refused(tmp_path):
    with pytest.raises(GoldInputError, match="no committed Silver run"):
        gold(tmp_path)
    (tmp_path / "silver" / "_runs").mkdir(parents=True)
    with pytest.raises(GoldInputError, match="no committed Silver run"):
        gold(tmp_path)


def test_orphan_partition_of_an_uncommitted_silver_run_is_never_read(tmp_path):
    fixture_silver(tmp_path)
    # A crashed Silver run "s9": partitions on disk, no manifest. Globbing
    # would pick it up (and its single bogus row would add a ROUTE-09 row).
    for table in ("telemetry_clean", "telemetry_quarantine", "telemetry_duplicates"):
        orphan = tmp_path / "silver" / table / "run_id=s9"
        orphan.mkdir(parents=True)
        (orphan / "part-00000.jsonl").write_text(
            '{"route_id":"ROUTE-09","event_time":"2025-01-01T00:00:00Z","delay_seconds":1,'
            '"_silver_event_time_status":"in_order","silver_run_id":"s9"}\n'
        )
    result = gold(tmp_path)
    assert result.manifest["silver_runs_consumed"][0]["run_id"] == "s1"
    assert gold_rows(result) == EXPECTED_ROWS
    with pytest.raises(GoldInputError, match="not committed"):
        gold(tmp_path, run_id="g2", silver_run_id="s9")


def test_a_newer_failed_silver_run_is_never_selected(tmp_path):
    fixture_silver(tmp_path)
    s1 = json.loads((tmp_path / "silver" / "_runs" / "s1.json").read_text())
    failed = {**s1, "run_id": "s2", "status": "FAILED", "processing_time": "2030-01-01T00:00:00.000000Z"}
    (tmp_path / "silver" / "_runs" / "s2.json").write_text(json.dumps(failed))
    result = gold(tmp_path)
    assert result.manifest["silver_runs_consumed"][0]["run_id"] == "s1"
    with pytest.raises(GoldInputError, match="not committed"):
        gold(tmp_path, run_id="g2", silver_run_id="s2")


def test_latest_is_by_processing_time_not_run_id(tmp_path):
    fixture_silver(tmp_path)  # s1 at 10:00
    # "s0" sorts before "s1" but is processed later -> it is the latest.
    silver(tmp_path / "bronze", tmp_path / "silver", run_id="s0",
           clock=fixed_clock(datetime(2025, 2, 1, 12, 0, tzinfo=UTC)))
    result = gold(tmp_path)
    assert result.manifest["silver_runs_consumed"][0]["run_id"] == "s0"
    assert result.manifest["silver_selection"] == "latest_committed"
    explicit = gold(tmp_path, run_id="g2", silver_run_id="s1")
    assert explicit.manifest["silver_selection"] == "explicit"
    assert explicit.manifest["silver_runs_consumed"][0]["run_id"] == "s1"


@pytest.mark.parametrize("role", ["clean", "quarantine", "duplicates"])
def test_tampered_silver_file_fails_loudly_and_commits_nothing(tmp_path, role):
    result = fixture_silver(tmp_path)
    path = tmp_path / "silver" / result.manifest["tables"][role]["path"]
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(GoldInputError, match="sha256"):
        gold(tmp_path)
    assert not _gold_committed(tmp_path)
    assert not (tmp_path / "gold" / "route_performance").exists()


def test_missing_silver_file_fails_loudly(tmp_path):
    result = fixture_silver(tmp_path)
    (tmp_path / "silver" / result.manifest["tables"]["clean"]["path"]).unlink()
    with pytest.raises(GoldInputError, match="missing"):
        gold(tmp_path)
    assert not _gold_committed(tmp_path)


def _rewrite_clean(tmp_path, result, transform):
    """Tamper with the clean file AND re-sign it in the Silver manifest, so
    only Gold's deeper checks can catch it."""
    entry = result.manifest["tables"]["clean"]
    path = tmp_path / "silver" / entry["path"]
    data = transform(path.read_bytes())
    path.write_bytes(data)
    manifest_path = tmp_path / "silver" / "_runs" / "s1.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"]["clean"]["sha256"] = hashlib.sha256(data).hexdigest()
    manifest_path.write_text(json.dumps(manifest))


def test_record_from_another_silver_run_is_rejected(tmp_path):
    result = fixture_silver(tmp_path)
    _rewrite_clean(tmp_path, result, lambda b: b.replace(b'"silver_run_id":"s1"', b'"silver_run_id":"sX"', 1))
    with pytest.raises(GoldInputError, match="silver_run_id"):
        gold(tmp_path)


def test_record_count_disagreeing_with_silver_counters_is_rejected(tmp_path):
    result = fixture_silver(tmp_path)
    _rewrite_clean(tmp_path, result, lambda b: b"".join(b.splitlines(keepends=True)[1:]))
    with pytest.raises(GoldInputError, match="lines"):
        gold(tmp_path)


def test_committed_manifest_with_failed_reconciliation_is_rejected(tmp_path):
    fixture_silver(tmp_path)
    path = tmp_path / "silver" / "_runs" / "s1.json"
    manifest = json.loads(path.read_text())
    manifest["reconciliation"]["holds"] = False
    path.write_text(json.dumps(manifest))
    with pytest.raises(GoldInputError, match="reconciliation"):
        gold(tmp_path)


def test_manifest_with_unsafe_table_path_is_rejected(tmp_path):
    fixture_silver(tmp_path)
    path = tmp_path / "silver" / "_runs" / "s1.json"
    manifest = json.loads(path.read_text())
    manifest["tables"]["clean"]["path"] = "../elsewhere/part-00000.jsonl"
    path.write_text(json.dumps(manifest))
    with pytest.raises(GoldInputError, match="unsafe"):
        gold(tmp_path)


def test_reference_data_must_match_what_silver_used(tmp_path):
    fixture_silver(tmp_path)
    other = load_scale_config().model_copy(update={"seed": 4242})
    with pytest.raises(GoldInputError, match="reference data"):
        gold(tmp_path, scale_config=other)
    assert not (tmp_path / "gold" / "_runs").exists()
