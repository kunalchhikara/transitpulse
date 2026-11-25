"""DE-11 criteria 5, 6, 9, 10 (identity failure) and the PROJECT.md section
21 bad-source / partial-failure / retry-exhaustion / reconciliation tests.
Failures are injected by dependency injection (`bronze_ingest`,
`extract_writer`, `hook`), never by production flags."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import time
from datetime import timedelta

from simulator.batch import DATASET_CATALOGUE
from simulator.bronze import BronzeStore, ingest_batch
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
    parse_ts,
    pk,
    rewrite_source_file,
    source_rows,
    strip,
    tree_bytes,
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
    assert outcome.watermark_after == outcome.watermark_before == control_before["watermark"]
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
    assert env.control(TARGET)["last_as_of"] == format_ts(first)  # X unchanged
    for dataset in INCREMENTAL_DATASETS:
        if dataset != TARGET:
            assert env.control(dataset)["last_as_of"] == format_ts(second)  # others keep their advance

    rerun = env.run(second)
    processed = [r.dataset for r in rerun.datasets if r.status != STATUS_UP_TO_DATE]
    assert processed == [TARGET]
    assert rerun.status == "SUCCEEDED"
    assert tree_bytes(env.root / "bronze") == expected_bronze
    assert tree_bytes(env.root / "state" / "control") == expected_control
    assert tree_bytes(env.root / "state" / "extracts") == expected_extracts
    assert bronze_keys(env.root / "bronze", TARGET) == expected_keys(env.root / "batch", TARGET, second)


def failing_inside_bronze(dataset: str):
    """A real Bronze failure (not a stand-in): Bronze's clock fails on its
    pre-commit read, so Bronze itself records a FAILED run manifest."""

    def ingest(batch_dir, *, clock, **kwargs):
        if batch_dir.parent.name != dataset:
            return ingest_batch(batch_dir, clock=clock, **kwargs)
        calls = []

        def failing_clock():
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("storage timeout inside Bronze")
            return clock()

        return ingest_batch(batch_dir, clock=failing_clock, **kwargs)

    return ingest


def test_failure_inside_bronze_then_rerun_keeps_data_control_and_extracts_identical(tmp_path, monkeypatch):
    """After DE-09, a dataset that failed *inside* Bronze gains a
    `previous_attempts` entry in its Bronze run manifest on the rerun; the
    Bronze data, every other manifest, control and extracts are
    byte-identical to an uninterrupted run."""
    clean = _relative_env(tmp_path / "clean", monkeypatch)
    cuts = all_watermarks(clean.batch_dir)
    first, second = cuts[len(cuts) // 3], cuts[-1]
    clean.run(first)
    clean.run(second)
    expected_bronze = tree_bytes(clean.root / "bronze")

    env = _relative_env(tmp_path / "bronze-failure", monkeypatch)
    env.run(first)
    failed = env.run(second, bronze_ingest=failing_inside_bronze(TARGET))
    assert failed.dataset(TARGET).status == STATUS_FAILED
    run_id = failed.dataset(TARGET).bronze_run_id
    assert BronzeStore(env.root / "bronze").read_manifest(run_id)["status"] == "FAILED"
    assert env.run(second).status == "SUCCEEDED"
    bronze = tree_bytes(env.root / "bronze")
    manifest_path = f"_runs/{run_id}.json"
    assert {k: v for k, v in bronze.items() if k != manifest_path} == {
        k: v for k, v in expected_bronze.items() if k != manifest_path
    }
    manifest = json.loads(bronze[manifest_path])
    assert manifest["status"] == "COMMITTED" and len(manifest["previous_attempts"]) == 1
    assert manifest["previous_attempts"][0]["status"] == "FAILED"
    assert tree_bytes(env.root / "state" / "control") == tree_bytes(clean.root / "state" / "control")
    assert tree_bytes(env.root / "state" / "extracts") == tree_bytes(clean.root / "state" / "extracts")


def test_bronze_manifests_written_through_the_loader_are_path_free(tmp_path):
    """DE-09: with absolute configured paths, no Bronze run manifest (incl. a
    FAILED attempt carried in `previous_attempts`) holds a local path."""
    env = make_env(tmp_path)  # absolute paths in config
    values = all_watermarks(env.batch_dir)
    env.run(values[2])
    env.run(values[-1], bronze_ingest=failing_inside_bronze(TARGET))
    env.run(values[-1])
    texts = [p.read_text() for p in (env.bronze_dir / "_runs").glob("*.json")]
    assert texts and any(json.loads(t)["previous_attempts"] for t in texts)  # guard: a retried run
    for text in texts:
        for needle in (str(tmp_path), str(tmp_path.resolve())):
            assert needle not in text
        for entry in json.loads(text)["input_files"]:
            assert "/" not in entry["path"] and "\\" not in entry["path"]


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

    # D2: the preserved rows are listed deterministically (line number in the
    # extract + sha256 of the line) in the audit record and the extract
    # manifest; the JSON objects with a missing/invalid updated_at land as
    # normal Bronze records, flagged for DE-12 to quarantine.
    extract_dir = env.state_dir / audit["extract_dir"]
    extract_lines = (extract_dir / f"{TARGET}.jsonl").read_bytes().split(b"\n")
    listed = audit["preserved_bad_rows"]
    expected = []
    for line, reason, outcome_kind in (
        (bad[0], "invalid_json", "bronze_parse_error_record"),
        (bad[1], "missing_watermark", "stored_record_requires_quarantine"),
        (bad[2], "invalid_watermark", "stored_record_requires_quarantine"),
    ):
        number = extract_lines.index(line) + 1
        expected.append({
            "extract_line": number, "reason": reason, "bronze_outcome": outcome_kind,
            "sha256": hashlib.sha256(line).hexdigest(),
        })
    assert [{k: row[k] for k in ("extract_line", "reason", "bronze_outcome", "sha256")} for row in listed] == expected
    assert [row["extract_line"] for row in listed] == sorted(row["extract_line"] for row in listed)
    manifest = json.loads((extract_dir / "manifest.json").read_text())
    assert manifest["datasets"][TARGET]["preserved_bad_rows"] == listed
    stored = [strip(r) for r in bronze_records(env.bronze_dir, TARGET) if r["run_id"] == audit["bronze_run_id"]]
    assert json.loads(bad[1]) in stored and json.loads(bad[2]) in stored  # normal Bronze records

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


LATE = "historical_trips"  # varied updated_at (a 1-day sample has one schedules publication time)


def _add_row_behind_the_watermark(env, dataset=LATE):
    """Append a row (new primary key) whose `updated_at` is strictly below
    the dataset's committed watermark: a window `updated_at > watermark` can
    never see it. Returns its primary key."""
    watermark = parse_ts(env.control(dataset)["watermark"])
    row = dict(next(r for r in source_rows(env.batch_dir, dataset) if parse_ts(r["updated_at"]) < watermark))
    key_column = DATASET_CATALOGUE[dataset].primary_key[0]
    row[key_column] = f"{row[key_column]}-LATE"
    data = (env.batch_dir / f"{dataset}.jsonl").read_bytes() + json.dumps(row, sort_keys=True).encode() + b"\n"
    rewrite_source_file(env.batch_dir, dataset, data)
    return pk(row, dataset)


def test_rows_appearing_behind_the_watermark_fail_the_dataset_by_default(tmp_path):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    env.run(values[len(values) // 2])
    control_before = env.control(LATE)
    _add_row_behind_the_watermark(env)
    result = env.run(values[-1])
    outcome = result.dataset(LATE)
    assert result.status == PIPELINE_PARTIAL
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "LateArrivalError"
    check = env.audit_records(result.pipeline_run_id, LATE)[0]["late_arrival_check"]
    assert check["delta"] == 1 and check["holds"] is False
    assert env.control(LATE) == control_before


def test_record_policy_surfaces_skipped_late_rows_instead_of_hiding_them(tmp_path):
    """O1: under late_arrival_policy=record the late rows are skipped, but
    the pipeline summary carries the total and a warnings status."""
    env = make_env(tmp_path, late_arrival_policy="record")
    values = all_watermarks(env.batch_dir)
    env.run(values[len(values) // 2])
    key = _add_row_behind_the_watermark(env)
    result = env.run(values[-1])
    assert result.status == "SUCCEEDED_WITH_WARNINGS"
    outcome = result.dataset(LATE)
    assert outcome.status == STATUS_COMMITTED and outcome.late_rows_skipped == 1
    summary = env.pipeline_summary(result)
    assert summary["status"] == "SUCCEEDED_WITH_WARNINGS"
    assert summary["late_rows_skipped"] == 1
    assert summary["warnings"] == [{"dataset": LATE, "type": "late_rows_skipped", "count": 1}]
    audit = env.audit_records(result.pipeline_run_id, LATE)[0]
    assert audit["late_rows_skipped"] == 1 and audit["late_arrival_check"]["holds"] is False
    assert key not in bronze_keys(env.bronze_dir, LATE)  # skipped, as the policy says
    assert env.control(LATE)["late_rows_skipped_total"] == 1
    # Reported once, in the run that skipped it; the next run is clean.
    again = env.run(values[-1] + timedelta(days=1))
    assert again.status == "SUCCEEDED" and env.pipeline_summary(again)["late_rows_skipped"] == 0
    assert env.control(LATE)["late_rows_skipped_total"] == 1
