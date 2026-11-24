"""DE-07 idempotency and determinism: same Silver input + same Gold run_id +
fixed clock -> byte-identical output (in-process and across CLI processes);
committed reruns are no-ops; conflicting reruns are refused; failed/crashed
attempts are retried with an audit trail; manifests hold no local paths."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from simulator.bronze import fixed_clock
from simulator.gold import GoldInputError, GoldRunIdConflictError
from simulator.tests._bronze_helpers import REPO_ROOT
from simulator.tests._gold_helpers import (
    EXPECTED_ROWS,
    GOLD_CONFIG,
    fixture_lines,
    fixture_silver,
    gold,
    gold_rows,
    silver_from_lines,
    store_files,
)
from simulator.tests._silver_helpers import silver

LATER = fixed_clock(datetime(2026, 6, 1, tzinfo=UTC))


def test_same_input_same_run_id_fixed_clock_is_byte_identical(tmp_path):
    fixture_silver(tmp_path / "a")
    fixture_silver(tmp_path / "b")
    gold(tmp_path / "a")
    gold(tmp_path / "b")
    files_a, files_b = store_files(tmp_path / "a" / "gold"), store_files(tmp_path / "b" / "gold")
    assert sorted(files_a) == [
        "_runs/g1.json",
        "route_performance/run_id=g1/part-00000.csv",
        "route_performance/run_id=g1/part-00000.jsonl",
    ]
    assert files_a == files_b


def test_committed_rerun_is_a_no_op_even_under_a_later_clock(tmp_path):
    fixture_silver(tmp_path)
    gold(tmp_path)
    before = store_files(tmp_path / "gold")
    again = gold(tmp_path, clock=LATER)
    assert again.status == "ALREADY_COMMITTED"
    assert store_files(tmp_path / "gold") == before


def test_new_silver_snapshot_under_the_same_gold_run_id_is_refused(tmp_path):
    fixture_silver(tmp_path)
    gold(tmp_path)
    # A newer Silver snapshot (different run, different processing_time).
    silver(tmp_path / "bronze", tmp_path / "silver", run_id="s2",
           clock=fixed_clock(datetime(2025, 2, 1, 10, 30, tzinfo=UTC)))
    with pytest.raises(GoldRunIdConflictError):
        gold(tmp_path)  # default selection is now s2
    # Explicitly selecting the Silver run g1 consumed is still a no-op...
    assert gold(tmp_path, silver_run_id="s1").status == "ALREADY_COMMITTED"
    # ...and a new Gold run_id aggregates the new snapshot.
    g2 = gold(tmp_path, run_id="g2")
    assert g2.manifest["silver_runs_consumed"][0]["run_id"] == "s2"
    assert [{**r, "gold_run_id": "g1", "silver_run_id": "s1"} for r in gold_rows(g2)] == EXPECTED_ROWS


def test_config_change_under_the_same_run_id_is_refused(tmp_path):
    fixture_silver(tmp_path)
    gold(tmp_path)
    with pytest.raises(GoldRunIdConflictError):
        gold(tmp_path, config=GOLD_CONFIG.model_copy(update={"period_minutes": 30}))


def test_failed_attempt_is_retried_and_kept_in_previous_attempts(tmp_path):
    silver_result = fixture_silver(tmp_path)
    clean = tmp_path / "silver" / silver_result.manifest["tables"]["clean"]["path"]
    original = clean.read_bytes()
    clean.write_bytes(original.replace(b'"delay_seconds":60', b'"delay_seconds":61'))
    with pytest.raises(GoldInputError, match="sha256"):
        gold(tmp_path)
    failed = json.loads((tmp_path / "gold" / "_runs" / "g1.json").read_text())
    assert failed["status"] == "FAILED" and failed["error"]["type"] == "GoldInputError"
    assert not (tmp_path / "gold" / "route_performance").exists()

    clean.write_bytes(original)
    result = gold(tmp_path)
    assert result.status == "COMMITTED"
    assert len(result.manifest["previous_attempts"]) == 1
    assert result.manifest["previous_attempts"][0]["error"]["type"] == "GoldInputError"
    assert gold_rows(result) == EXPECTED_ROWS


def test_crash_leftovers_are_removed_and_recorded(tmp_path):
    fixture_silver(tmp_path)
    orphan = tmp_path / "gold" / "route_performance" / "run_id=g1"
    orphan.mkdir(parents=True)
    (orphan / "part-00000.jsonl").write_text('{"half":"written"}\n')
    staging = tmp_path / "gold" / "_staging" / "g1" / "route_performance"
    staging.mkdir(parents=True)
    result = gold(tmp_path)
    assert result.manifest["removed_uncommitted_paths"] == ["_staging/g1", "route_performance/run_id=g1"]
    assert gold_rows(result) == EXPECTED_ROWS
    assert not (tmp_path / "gold" / "_staging").exists()


def test_manifest_holds_no_local_paths(tmp_path):
    fixture_silver(tmp_path)
    text = gold(tmp_path).manifest_path.read_text()
    assert str(tmp_path) not in text
    assert str(REPO_ROOT) not in text
    assert "/Users/" not in text and "/home/" not in text


def test_two_cli_processes_produce_byte_identical_stores(tmp_path):
    for name in ("a", "b"):
        silver_from_lines(tmp_path / name, fixture_lines())
        done = subprocess.run(
            [sys.executable, "-m", "simulator.gold", "--run-id", "g1", "--silver-dir", str(tmp_path / name / "silver"),
             "--output-dir", str(tmp_path / name / "gold"), "--fixed-processing-time", "2025-02-01T11:00:00Z"],
            cwd=REPO_ROOT, check=True, capture_output=True, text=True,
        )
        assert json.loads(done.stdout)["status"] == "COMMITTED"
    assert store_files(tmp_path / "a" / "gold") == store_files(tmp_path / "b" / "gold")
    assert gold_rows_from(tmp_path / "a" / "gold") == EXPECTED_ROWS


def gold_rows_from(root):
    path = root / "route_performance" / "run_id=g1" / "part-00000.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_moving_the_stores_does_not_change_the_output(tmp_path):
    fixture_silver(tmp_path / "a")
    shutil.copytree(tmp_path / "a", tmp_path / "moved")
    gold(tmp_path / "a")
    gold(tmp_path / "moved")
    assert store_files(tmp_path / "a" / "gold") == store_files(tmp_path / "moved" / "gold")
