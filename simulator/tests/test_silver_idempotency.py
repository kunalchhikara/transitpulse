"""DE-06 idempotency (README "DE-06: Idempotency and reruns"):

* same Bronze inputs + same Silver run_id + fixed clock -> byte-identical
  output, in-process and across separate CLI processes
* same run_id already committed with the same input   -> no-op
* same run_id, different input/config                 -> conflict, untouched
* same run_id after a FAILED / crashed attempt        -> clean retry, the
  failed attempt kept in `previous_attempts`
* new run_id                                          -> new snapshot
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from simulator.bronze import fixed_clock
from simulator.silver import SilverRunIdConflictError, load_silver_config
from simulator.silver import pipeline as silver_pipeline
from simulator.tests._bronze_helpers import REPO_ROOT, incident_stream_lines
from simulator.tests._silver_helpers import bronze_from_lines, silver, table

LATER = fixed_clock(datetime(2026, 1, 1, tzinfo=UTC))


def _snapshot(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def bronze(tmp_path):
    return bronze_from_lines(tmp_path / "bronze", incident_stream_lines(150))


def test_same_inputs_run_id_and_clock_give_byte_identical_output(tmp_path, bronze):
    silver(bronze, tmp_path / "a")
    silver(bronze, tmp_path / "b")
    a, b = _snapshot(tmp_path / "a"), _snapshot(tmp_path / "b")
    assert a == b
    assert set(a) == {
        "_runs/s1.json",
        "telemetry_clean/run_id=s1/part-00000.jsonl",
        "telemetry_quarantine/run_id=s1/part-00000.jsonl",
        "telemetry_duplicates/run_id=s1/part-00000.jsonl",
    }


def test_byte_identical_across_separate_cli_processes(tmp_path, bronze):
    for name in ("p1", "p2"):
        done = subprocess.run(
            [sys.executable, "-m", "simulator.silver", "--run-id", "s1", "--bronze-dir", str(bronze),
             "--output-dir", str(tmp_path / name), "--fixed-processing-time", "2025-02-01T10:00:00Z"],
            cwd=REPO_ROOT, capture_output=True, check=True,
        )
        assert json.loads(done.stdout)["status"] == "COMMITTED"
    assert _snapshot(tmp_path / "p1") == _snapshot(tmp_path / "p2")
    assert _snapshot(tmp_path / "p1") == _snapshot(silver(bronze, tmp_path / "inproc").manifest_path.parent.parent)


def test_rerun_of_committed_run_is_a_noop(tmp_path, bronze):
    out = tmp_path / "silver"
    first = silver(bronze, out)
    before = _snapshot(out)
    second = silver(bronze, out, clock=LATER)  # a later clock must not leak in
    assert (first.status, second.status) == ("COMMITTED", "ALREADY_COMMITTED")
    assert second.manifest == first.manifest
    assert _snapshot(out) == before


def test_same_run_id_with_different_input_is_refused(tmp_path, bronze):
    out = tmp_path / "silver"
    silver(bronze, out)
    before = _snapshot(out)
    bronze_from_lines(bronze, incident_stream_lines(20)[:5], run_id="b2", clock=LATER)
    with pytest.raises(SilverRunIdConflictError):
        silver(bronze, out)  # default selection now includes b2
    changed = load_silver_config().model_copy(update={"allowed_lateness_seconds": 5})
    with pytest.raises(SilverRunIdConflictError):
        silver(bronze, out, config=changed, bronze_run_ids=["b1"])
    assert _snapshot(out) == before
    # The same selection as before is still a no-op.
    assert silver(bronze, out, bronze_run_ids=["b1"]).status == "ALREADY_COMMITTED"


def test_new_run_id_is_an_independent_snapshot(tmp_path, bronze):
    out = tmp_path / "silver"
    first = silver(bronze, out, run_id="s1")
    second = silver(bronze, out, run_id="s2", clock=LATER)
    assert second.status == "COMMITTED"
    assert second.manifest["counters"]["extracted"] == first.manifest["counters"]["extracted"]
    for role in ("clean", "quarantine", "duplicates"):
        drop = {"silver_run_id", "processing_time"}
        rows1 = [{k: v for k, v in r.items() if k not in drop} for r in table(first, role)]
        rows2 = [{k: v for k, v in r.items() if k not in drop} for r in table(second, role)]
        assert rows1 == rows2
    assert {r["silver_run_id"] for r in table(second, "clean")} == {"s2"}


def test_failed_attempt_is_retried_cleanly_and_audited(tmp_path, bronze, monkeypatch):
    out = tmp_path / "silver"

    def boom(*args, **kwargs):
        raise RuntimeError("disk full (simulated)")

    monkeypatch.setattr(silver_pipeline, "reconcile", boom)
    with pytest.raises(RuntimeError):
        silver(bronze, out)
    failed = json.loads((out / "_runs" / "s1.json").read_text())
    assert failed["status"] == "FAILED" and failed["error"]["type"] == "RuntimeError"
    assert not (out / "_staging").exists()
    monkeypatch.undo()

    retried = silver(bronze, out)
    assert retried.status == "COMMITTED"
    assert retried.manifest["previous_attempts"] == [
        {"started_at": failed["started_at"], "ended_at": failed["ended_at"],
         "input_fingerprint": failed["input_fingerprint"], "error": failed["error"]}
    ]
    # Apart from the audit trail, output equals a never-failed run.
    clean_run = silver(bronze, tmp_path / "clean")
    a, b = _snapshot(out), _snapshot(tmp_path / "clean")
    assert {k: v for k, v in a.items() if k != "_runs/s1.json"} == {k: v for k, v in b.items() if k != "_runs/s1.json"}
    assert retried.manifest["counters"] == clean_run.manifest["counters"]


def test_crash_leftovers_are_removed_before_retry(tmp_path, bronze):
    out = tmp_path / "silver"
    silver(bronze, tmp_path / "reference")
    # Simulate a crash after publishing partitions but before the manifest.
    shutil.copytree(tmp_path / "reference", out)
    (out / "_runs" / "s1.json").unlink()
    (out / "_staging" / "s1" / "telemetry_clean").mkdir(parents=True)
    result = silver(bronze, out)
    assert result.status == "COMMITTED"
    assert sorted(result.manifest["removed_uncommitted_paths"]) == [
        "_staging/s1",
        "telemetry_clean/run_id=s1",
        "telemetry_duplicates/run_id=s1",
        "telemetry_quarantine/run_id=s1",
    ]
    assert _snapshot(out) == {
        k: v for k, v in _snapshot(tmp_path / "reference").items() if k != "_runs/s1.json"
    } | {"_runs/s1.json": (out / "_runs" / "s1.json").read_bytes()}


def test_manifest_contains_no_absolute_local_paths(tmp_path, bronze):
    text = silver(bronze, tmp_path / "silver").manifest_path.read_text()
    assert str(tmp_path) not in text
