"""DE-11 criteria 5, 6, 9, 10 (identity failure) and the PROJECT.md section
21 bad-source / partial-failure / retry-exhaustion / reconciliation tests.
Failures are injected by dependency injection (`bronze_ingest`,
`extract_writer`, `hook`), never by production flags."""

from __future__ import annotations

import dataclasses
import json
import time

import pytest

from simulator.bronze import ingest_batch
from simulator.incremental import (
    PIPELINE_PARTIAL,
    STATUS_COMMITTED,
    STATUS_FAILED,
    STATUS_UP_TO_DATE,
    ManualClock,
    TransientError,
    format_ts,
    write_extract,
)
from simulator.tests._incremental_helpers import (
    FIXED_NOW,
    INCREMENTAL_DATASETS,
    all_watermarks,
    bronze_keys,
    bronze_records,
    committed_run_ids,
    expected_keys,
    make_env,
    rewrite_source_file,
    source_rows,
    tree_bytes,
    write_batch_source,
)

TARGET = "historical_trips"


def failing_ingest(dataset: str, errors: list[BaseException]):
    """Bronze stand-in: raise the queued errors for `dataset`, then delegate
    to the real `ingest_batch`."""
    queue = list(errors)

    def ingest(batch_dir, **kwargs):
        if batch_dir.parent.name == dataset and queue:
            raise queue.pop(0)
        return ingest_batch(batch_dir, **kwargs)

    return ingest


def always_failing_ingest(dataset: str, error_factory):
    def ingest(batch_dir, **kwargs):
        if batch_dir.parent.name == dataset:
            raise error_factory()
        return ingest_batch(batch_dir, **kwargs)

    return ingest


# -- retries -----------------------------------------------------------------


def test_transient_failures_are_retried_with_configured_backoff_on_the_injected_clock(tmp_path):
    env = make_env(tmp_path)
    clock = ManualClock(FIXED_NOW)
    as_of = all_watermarks(env.batch_dir)[-1]
    started = time.monotonic()
    result = env.run(as_of, clock=clock, bronze_ingest=failing_ingest(TARGET, [TimeoutError("t1"), ConnectionError("t2")]))
    assert time.monotonic() - started < 5  # backoff 2 + 4 s was virtual, not slept
    retry = env.config.retry
    assert clock.sleeps == [retry.backoff_for(1), retry.backoff_for(2)]
    assert result.status == "SUCCEEDED"
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_COMMITTED and outcome.attempts == 3
    audit = env.audit_records(result.pipeline_run_id, TARGET)
    assert [a["attempt"] for a in audit] == [1, 2, 3]
    assert [a["status"] for a in audit] == [STATUS_FAILED, STATUS_FAILED, STATUS_COMMITTED]
    assert [a["error"]["type"] for a in audit[:2]] == ["TimeoutError", "ConnectionError"]
    assert all(a["error"]["retryable"] and a["error"]["will_retry"] for a in audit[:2])
    assert [a["next_backoff_seconds"] for a in audit] == [retry.backoff_for(1), retry.backoff_for(2), None]
    # Attempt start times move with the virtual backoff.
    assert audit[1]["started_at"] > audit[0]["started_at"]
    assert all(a["watermark_after"] is None for a in audit[:2])
    assert bronze_keys(env.bronze_dir, TARGET) == expected_keys(env.batch_dir, TARGET, as_of)


def test_retry_exhaustion_fails_the_dataset_with_watermark_unchanged(tmp_path):
    env = make_env(tmp_path).with_config(
        retry={"max_attempts": 4, "initial_backoff_seconds": 1, "backoff_multiplier": 3, "max_backoff_seconds": 5}
    )
    values = all_watermarks(env.batch_dir)
    env.run(values[2])
    control_before = env.control(TARGET)
    clock = ManualClock(FIXED_NOW)
    result = env.run(values[-1], clock=clock, bronze_ingest=always_failing_ingest(TARGET, lambda: TransientError("flaky")))
    assert clock.sleeps == [1, 3, 5]
    assert result.status == PIPELINE_PARTIAL
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.attempts == 4
    assert outcome.watermark_after == outcome.watermark_before == format_ts(values[2])
    assert env.control(TARGET) == control_before
    audit = env.audit_records(result.pipeline_run_id, TARGET)
    assert len(audit) == 4 and not audit[-1]["error"]["will_retry"]
    assert bronze_keys(env.bronze_dir, TARGET) == expected_keys(env.batch_dir, TARGET, values[2])


def test_non_transient_errors_are_not_retried(tmp_path):
    env = make_env(tmp_path)
    clock = ManualClock(FIXED_NOW)
    result = env.run(
        all_watermarks(env.batch_dir)[-1], clock=clock,
        bronze_ingest=always_failing_ingest(TARGET, lambda: RuntimeError("permanent")),
    )
    assert clock.sleeps == []
    assert result.dataset(TARGET).attempts == 1
    assert result.dataset(TARGET).error["retryable"] is False


# -- partial failure -----------------------------------------------------------


def _relative_env(root, monkeypatch, **overrides):
    env = make_env(root, relative=True, **overrides)
    monkeypatch.chdir(root)
    return env


def test_partial_failure_then_rerun_matches_an_uninterrupted_run_byte_for_byte(tmp_path, monkeypatch):
    clean = _relative_env(tmp_path / "clean", monkeypatch)
    cuts = all_watermarks(clean.batch_dir)
    first, second = cuts[len(cuts) // 3], cuts[-1]
    clean.run(first)
    clean.run(second)
    expected_bronze = tree_bytes(clean.root / "bronze")
    expected_control = tree_bytes(clean.root / "state" / "control")
    expected_extracts = tree_bytes(clean.root / "state" / "extracts")

    env = _relative_env(tmp_path / "partial", monkeypatch)
    env.run(first)
    result = env.run(second, bronze_ingest=always_failing_ingest(TARGET, lambda: RuntimeError("disk on fire")))
    assert result.status == PIPELINE_PARTIAL
    assert result.dataset(TARGET).status == STATUS_FAILED
    assert env.control(TARGET)["watermark"] == format_ts(first)  # X unchanged
    for dataset in INCREMENTAL_DATASETS:
        if dataset != TARGET:
            assert env.control(dataset)["watermark"] == format_ts(second)  # others keep their advance

    rerun = env.run(second)
    processed = [r.dataset for r in rerun.datasets if r.status != STATUS_UP_TO_DATE]
    assert processed == [TARGET]
    assert rerun.status == "SUCCEEDED"
    assert tree_bytes(env.root / "bronze") == expected_bronze
    assert tree_bytes(env.root / "state" / "control") == expected_control
    assert tree_bytes(env.root / "state" / "extracts") == expected_extracts
    assert bronze_keys(env.root / "bronze", TARGET) == expected_keys(env.root / "batch", TARGET, second)


# -- bad source ----------------------------------------------------------------


def _corrupt(env, dataset=TARGET):
    """Append three bad lines: unparseable JSON, missing and unparseable
    updated_at. Returns the bad lines."""
    original = (env.batch_dir / f"{dataset}.jsonl").read_bytes()
    row = source_rows(env.batch_dir, dataset)[0]
    missing = {k: v for k, v in row.items() if k != "updated_at"}
    unparseable = {**row, "updated_at": "not-a-timestamp", "trip_id": "TRIP-BAD"}
    bad = [b'{"trip_id": "TRIP-TRUNC", "updated_at": "2025-01-0', json.dumps(missing).encode(), json.dumps(unparseable).encode()]
    lines = original.splitlines()
    data = b"\n".join([lines[0], bad[0], *lines[1:], bad[1], bad[2]]) + b"\n"
    rewrite_source_file(env.batch_dir, dataset, data)
    return bad


def test_bad_source_strict_fails_the_dataset_and_lands_nothing(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[-1]
    _corrupt(env)
    result = env.run(as_of)
    assert result.status == PIPELINE_PARTIAL
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "BadSourceError" and outcome.attempts == 1
    assert env.control(TARGET) is None
    assert bronze_records(env.bronze_dir, TARGET) == []
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    assert audit["rejected_count"] == 3 and audit["bad_source_rows"] == 3
    assert audit["bad_source_rows_by_reason"] == {"invalid_json": 1, "missing_watermark": 1, "invalid_watermark": 1}
    assert all(r.status == STATUS_COMMITTED for r in result.datasets if r.dataset != TARGET)


def test_bad_source_preserve_lands_each_bad_line_once_and_counts_it(tmp_path):
    env = make_env(tmp_path, bad_source_policy="preserve")
    values = all_watermarks(env.batch_dir)
    first = values[len(values) // 2]
    good_rows = len(source_rows(env.batch_dir, TARGET))
    in_window = sum(expected_keys(env.batch_dir, TARGET, first).values())  # computed before corruption
    bad = _corrupt(env)
    result = env.run(first)
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_COMMITTED
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    assert audit["bad_rows_preserved"] == 3
    assert audit["bad_source_rows_by_reason"] == {"invalid_json": 1, "missing_watermark": 1, "invalid_watermark": 1}
    assert audit["source_rows_in_window"] == in_window
    assert audit["bronze_parse_error_records"] == 1  # the unparseable line
    assert audit["extracted_count"] == audit["source_rows_in_window"] + 3 == audit["bronze_records_written"]
    assert audit["reconciliation"]["holds"] is True
    assert audit["rejected_count"] == 0
    raw = [r.get("_bronze_raw_line") for r in bronze_records(env.bronze_dir, TARGET) if "_bronze_parse_error" in r]
    assert raw == [bad[0].decode()]

    second = env.run(values[-1])
    audit2 = env.audit_records(second.pipeline_run_id, TARGET)[0]
    assert audit2["bad_rows_preserved"] == 0  # landed once only
    records = bronze_records(env.bronze_dir, TARGET)
    assert len(records) == good_rows + 3  # every source line exactly once (good + 3 bad)
    assert env.control(TARGET)["preserved_bad_lines"] and sum(env.control(TARGET)["preserved_bad_lines"].values()) == 3


def test_source_manifest_mismatch_fails_unless_verification_is_off(tmp_path):
    env = make_env(tmp_path)
    path = env.batch_dir / f"{TARGET}.jsonl"
    data = path.read_bytes()
    path.write_bytes(data[: data.rindex(b"\n", 0, len(data) - 1) + 1])  # truncated transfer, manifest untouched
    as_of = all_watermarks(env.batch_dir)[-1]
    result = env.run(as_of)
    assert result.dataset(TARGET).error["type"] == "SourceManifestMismatchError"
    assert env.control(TARGET) is None
    relaxed = env.with_config(verify_source_manifest=False)
    result = relaxed.run(as_of)
    assert result.dataset(TARGET).status == STATUS_COMMITTED
    audit = relaxed.audit_records(result.pipeline_run_id, TARGET)[0]
    assert audit["source_manifest_check"]["status"] == "MISMATCH"


def test_missing_source_file_or_dataset_fails_only_that_dataset(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[-1]
    (env.ref_dir / "stops.json").unlink()
    manifest = json.loads((env.batch_dir / "manifest.json").read_text())
    del manifest["datasets"][TARGET]
    (env.batch_dir / "manifest.json").write_text(json.dumps(manifest))
    result = env.run(as_of)
    assert result.status == PIPELINE_PARTIAL
    assert {r.dataset for r in result.datasets if r.status == STATUS_FAILED} == {"stops", TARGET}
    assert result.dataset("stops").error["type"] == "SourceError"


# -- reconciliation identity -----------------------------------------------------


def test_extract_that_loses_a_row_fails_before_bronze(tmp_path):
    env = make_env(tmp_path)

    def lossy_writer(directory, plan, lines, **kwargs):
        if plan.name == TARGET:
            lines = list(lines)[:-1]  # drops a row but writes a self-consistent manifest
        return write_extract(directory, plan, lines, **kwargs)

    result = env.run(all_watermarks(env.batch_dir)[-1], extract_writer=lossy_writer)
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "ReconciliationError"
    assert env.control(TARGET) is None
    assert not [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")]
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    assert audit["reconciliation"]["holds"] is False
    assert audit["extracted_count"] == audit["source_rows_in_window"] - 1


def test_bronze_count_disagreeing_with_the_extract_fails_and_keeps_the_watermark(tmp_path):
    env = make_env(tmp_path)

    def short_counting_ingest(batch_dir, **kwargs):
        result = ingest_batch(batch_dir, **kwargs)
        if batch_dir.parent.name != TARGET:
            return result
        manifest = json.loads(json.dumps(result.manifest))
        manifest["tables"][TARGET]["records_written"] -= 1
        return dataclasses.replace(result, manifest=manifest)

    result = env.run(all_watermarks(env.batch_dir)[-1], bronze_ingest=short_counting_ingest)
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "ReconciliationError"
    assert outcome.watermark_after is None and env.control(TARGET) is None
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    rec = audit["reconciliation"]
    assert rec["holds"] is False and rec["bronze_records_written"] == rec["extracted_count"] - 1


# -- late arrivals (source completeness) -----------------------------------------


@pytest.mark.parametrize("policy", ["fail", "record"])
def test_rows_appearing_behind_the_watermark_are_detected(tmp_path, policy):
    """A one-day source extracted at its last updated_at, then regenerated
    with more days: the new day's schedules were published (updated_at)
    before that watermark, so a window can never see them - loud failure
    (default) or recorded."""
    env = make_env(tmp_path, late_arrival_policy=policy)
    last = all_watermarks(env.batch_dir)[-1]
    env.run(last)
    write_batch_source(env.batch_dir, service_days=2)
    result = env.run(all_watermarks(env.batch_dir)[-1])
    outcome = result.dataset("schedules")
    audit = env.audit_records(result.pipeline_run_id, "schedules")[0]
    check = audit["late_arrival_check"]
    assert check["delta"] > 0 and check["holds"] is False
    if policy == "fail":
        assert outcome.status == STATUS_FAILED and outcome.error["type"] == "LateArrivalError"
        assert env.control("schedules")["watermark"] == format_ts(last)
    else:
        assert outcome.status == STATUS_COMMITTED
