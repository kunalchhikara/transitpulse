"""DE-11 criteria 10 and 12: one immutable PROJECT.md section 15 audit record
per attempt with the reconciliation identity; no absolute paths in
control/audit files (FU-04); every log line carries the run id (FU-09)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from simulator.bronze import ingest_batch
from simulator.incremental import (
    RECONCILIATION_IDENTITY,
    STATUS_COMMITTED,
    AuditStore,
    StateError,
    TransientError,
)
from simulator.incremental.state import write_immutable
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    REFERENCE_ENTITIES,
    all_watermarks,
    make_env,
    tree_bytes,
)

SECTION_15_FIELDS = (
    "pipeline_run_id", "dataset", "attempt", "started_at", "ended_at", "duration_seconds",
    "window_low", "window_high", "watermark_before", "watermark_after", "source_rows_in_window",
    "extracted_count", "bronze_records_written", "rejected_count", "bronze_parse_error_records",
    "status", "error",
)


def test_one_audit_record_per_attempt_with_section_15_fields(tmp_path):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    env.run(values[2])
    result = env.run(values[-1])
    for dataset in INCREMENTAL_DATASETS:
        records = env.audit_records(result.pipeline_run_id, dataset)
        assert len(records) == 1
        record = records[0]
        for field in SECTION_15_FIELDS:
            assert field in record, field
        assert record["status"] == STATUS_COMMITTED and record["error"] is None
        for field in SECTION_15_FIELDS:
            if field not in ("error",):
                assert record[field] is not None, (dataset, field)
        rec = record["reconciliation"]
        assert rec["identity"] == RECONCILIATION_IDENTITY
        assert rec["holds"] is True
        assert record["source_rows_in_window"] == record["extracted_count"] == record["bronze_records_written"]
        assert record["watermark_before"] == record["window_low"]
        assert record["watermark_after"] == record["window_high"] == result.as_of
    summary = json.loads((env.state_dir / result.summary_path).read_text())
    assert summary["status"] == "SUCCEEDED" and len(summary["datasets"]) == len(INCREMENTAL_DATASETS) + 4


def test_records_are_never_overwritten_and_each_run_gets_its_own_directory(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[-1]
    first = env.run(as_of)
    snapshot = tree_bytes(env.state_dir / "audit" / first.pipeline_run_id)
    second = env.run(as_of)
    third = env.run(as_of)
    assert len({first.pipeline_run_id, second.pipeline_run_id, third.pipeline_run_id}) == 3
    assert tree_bytes(env.state_dir / "audit" / first.pipeline_run_id) == snapshot
    path = env.state_dir / "audit" / first.pipeline_run_id / "schedules" / "attempt-001.json"
    with pytest.raises(StateError):
        write_immutable(path, "{}")
    assert tree_bytes(env.state_dir / "audit" / first.pipeline_run_id) == snapshot
    with pytest.raises(StateError):
        env.run(as_of, pipeline_run_id=first.pipeline_run_id)
    assert tree_bytes(env.state_dir / "audit" / first.pipeline_run_id) == snapshot
    with pytest.raises(StateError):
        AuditStore(env.state_dir).write_pipeline_summary({"pipeline_run_id": first.pipeline_run_id})


def test_retried_attempts_each_keep_their_own_record(tmp_path):
    env = make_env(tmp_path)
    calls = []

    def flaky(batch_dir, **kwargs):
        if batch_dir.parent.name == "schedules" and len(calls) < 2:
            calls.append(1)
            raise TransientError("try again")
        return ingest_batch(batch_dir, **kwargs)

    result = env.run(all_watermarks(env.batch_dir)[-1], bronze_ingest=flaky)
    records = env.audit_records(result.pipeline_run_id, "schedules")
    assert [r["attempt"] for r in records] == [1, 2, 3]
    assert [r["status"] for r in records] == ["FAILED", "FAILED", "COMMITTED"]
    assert records[0]["error"]["message"] == "try again"


def _strings(value):
    if isinstance(value, dict):
        for k, v in value.items():
            yield k
            yield from _strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from _strings(v)
    elif isinstance(value, str):
        yield value


def test_control_and_audit_files_hold_no_absolute_paths(tmp_path):
    env = make_env(tmp_path)  # absolute paths in config
    values = all_watermarks(env.batch_dir)

    def leaky(batch_dir, **kwargs):
        if batch_dir.parent.name == "routes":
            raise RuntimeError(f"could not open {Path(batch_dir).resolve()} (home {Path.home()})")
        return ingest_batch(batch_dir, **kwargs)

    (env.ref_dir / "stops.json").unlink()  # error path with a file name
    failed = env.run(values[1], bronze_ingest=leaky)
    env.run(values[-1])
    files = [*(env.state_dir / "control").glob("*.json"), *(env.state_dir / "audit").rglob("*.json")]
    assert files
    leaked = []
    for path in files:
        text = path.read_text()
        for needle in (str(tmp_path), str(tmp_path.resolve()), str(Path.home())):
            if needle in text:
                leaked.append((path.name, needle))
        for s in _strings(json.loads(text)):
            if s.startswith("/") or (len(s) > 2 and s[1] == ":" and s[2] in "\\/"):
                leaked.append((path.name, s))
    assert leaked == []
    message = env.audit_records(failed.pipeline_run_id, "routes")[0]["error"]["message"]
    assert "<state_dir>" in message  # sanitised, still informative


def test_every_log_line_carries_the_pipeline_run_id(tmp_path, caplog):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    caplog.set_level(logging.DEBUG)
    first = env.run(values[3])
    second = env.run(values[-1], bronze_ingest=lambda d, **kw: (_ for _ in ()).throw(RuntimeError("x"))
                     if d.parent.name == "vehicles" else ingest_batch(d, **kw))
    records = [r for r in caplog.records if r.name in ("simulator.incremental", "simulator.bronze")]
    assert {r.name for r in records} == {"simulator.incremental", "simulator.bronze"}
    assert all(getattr(r, "pipeline_run_id", None) for r in records)
    assert {r.pipeline_run_id for r in records} == {first.pipeline_run_id, second.pipeline_run_id}
    dataset_lines = [r for r in records if r.name == "simulator.bronze"]
    assert all(getattr(r, "dataset", None) in (*REFERENCE_ENTITIES, *INCREMENTAL_DATASETS) for r in dataset_lines)
