"""DE-09 criterion 1 (BACKLOG FU-03): retrying a FAILED Bronze run_id keeps
every earlier failed attempt in the manifest's `previous_attempts`.

Shape: one entry per earlier FAILED attempt, oldest first, carrying the keys
Silver/Gold record (`started_at`, `ended_at`, `input_fingerprint`, `error`)
plus `status`, `duration_seconds` and the attempt's `totals` (counts; null
when the attempt failed before counting finished). Every expected entry here
is built from the FAILED manifest read back from disk *before* the retry -
never from the code under test's own bookkeeping.
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime

import pytest

from simulator.bronze import (
    BronzeStore,
    DuplicateInputError,
    MalformedInputError,
    ParseErrorPolicy,
    SourceManifestError,
    fixed_clock,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.bronze.ingest import accounting_identity_holds
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    SAMPLE_BATCH_DIR,
    clean_stream_lines,
    run_cli,
    ticking_clock,
    write_lines,
)

TABLE = load_bronze_config().stream.table
STRICT = load_bronze_config().model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
T0 = datetime(2025, 3, 1, 8, 0, 0, tzinfo=UTC)

#: Keys Silver (DE-06) and Gold (DE-07) record per previous attempt.
SILVER_GOLD_ATTEMPT_KEYS = ("started_at", "ended_at", "input_fingerprint", "error")
#: Bronze records those plus status, duration and counts (DE-09 criterion 1).
ATTEMPT_KEYS = (*SILVER_GOLD_ATTEMPT_KEYS, "status", "duration_seconds", "totals")


def expected_entry(failed_manifest: dict) -> dict:
    assert failed_manifest["status"] == "FAILED"
    return {key: failed_manifest[key] for key in ATTEMPT_KEYS}


def same_time_as(result):
    """A fixed clock at `result`'s ingestion time, for a never-failed
    reference run whose data files must be byte-identical."""
    return fixed_clock(datetime.fromisoformat(result.manifest["ingestion_time"]))


def _data_files(root):
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and "_runs" not in p.parts
    }


def _fail_on_publish(monkeypatch, message="disk full"):
    def boom(self, table, run_id):
        raise OSError(message)

    monkeypatch.setattr(BronzeStore, "publish_table", boom)


def test_first_commit_has_empty_previous_attempts(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(5))
    result = ingest_stream([src], run_id="r1", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)
    assert result.manifest["previous_attempts"] == []


def test_two_failures_then_success_keeps_exactly_two_attempts(tmp_path, monkeypatch):
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    clock = ticking_clock(T0)
    lines = clean_stream_lines(12)
    src = tmp_path / "events.ndjson"

    # Attempt 1: strict policy, truncated final line -> MalformedInputError
    # (counted first, so the attempt's totals are known).
    src.write_bytes(("\n".join(lines) + '\n{"event_id": "E').encode())
    with pytest.raises(MalformedInputError):
        ingest_stream([src], run_id="r1", output_dir=out, clock=clock, config=STRICT)
    failed1 = store.read_manifest("r1")
    assert failed1["previous_attempts"] == []
    assert failed1["totals"]["parse_error_records"] == 1

    # Attempt 2: input fixed, but a transient I/O failure during commit.
    write_lines(src, lines)
    _fail_on_publish(monkeypatch)
    with pytest.raises(OSError, match="disk full"):
        ingest_stream([src], run_id="r1", output_dir=out, clock=clock, config=STRICT)
    monkeypatch.undo()
    failed2 = store.read_manifest("r1")
    assert failed2["status"] == "FAILED"
    assert failed2["previous_attempts"] == [expected_entry(failed1)]
    assert failed2["input_fingerprint"] != failed1["input_fingerprint"]

    # Attempt 3: succeeds.
    result = ingest_stream([src], run_id="r1", output_dir=out, clock=clock, config=STRICT)
    assert result.status == "COMMITTED"
    history = result.manifest["previous_attempts"]
    assert len(history) == 2
    assert history == [expected_entry(failed1), expected_entry(failed2)]
    assert [h["error"]["type"] for h in history] == ["MalformedInputError", "OSError"]
    assert [h["status"] for h in history] == ["FAILED", "FAILED"]
    # Distinct, ordered attempt times from the ticking clock.
    times = [(h["started_at"], h["ended_at"]) for h in history] + [
        (result.manifest["started_at"], result.manifest["ended_at"])
    ]
    flat = [t for pair in times for t in pair]
    assert flat == sorted(flat) and len(set(flat)) == len(flat)
    for h in history:
        assert set(SILVER_GOLD_ATTEMPT_KEYS) <= set(h)
        if h["totals"] is not None:
            assert accounting_identity_holds(h["totals"])
    assert json.loads(store.manifest_path("r1").read_text()) == result.manifest

    # Accounting identities and data equal a never-failed run.
    clean = ingest_stream([src], run_id="r1", output_dir=tmp_path / "clean", clock=same_time_as(result), config=STRICT)
    assert result.manifest["totals"] == clean.manifest["totals"]
    assert result.manifest["totals"]["records_written"] == len(lines)
    assert accounting_identity_holds(result.manifest["totals"])
    assert result.manifest["accounting_identity_holds"] is True
    assert _data_files(out) == _data_files(tmp_path / "clean")
    assert len(list(store.read_table(TABLE))) == len(lines)


def test_already_committed_noop_adds_no_attempt(tmp_path, monkeypatch):
    out = tmp_path / "bronze"
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(6))
    clock = ticking_clock(T0)
    _fail_on_publish(monkeypatch)
    with pytest.raises(OSError):
        ingest_stream([src], run_id="r1", output_dir=out, clock=clock)
    monkeypatch.undo()
    committed = ingest_stream([src], run_id="r1", output_dir=out, clock=clock)
    assert len(committed.manifest["previous_attempts"]) == 1
    before = BronzeStore(out).manifest_path("r1").read_bytes()

    again = ingest_stream([src], run_id="r1", output_dir=out, clock=clock)
    assert again.status == "ALREADY_COMMITTED"
    assert again.manifest["previous_attempts"] == committed.manifest["previous_attempts"]
    assert BronzeStore(out).manifest_path("r1").read_bytes() == before


def test_batch_retry_history_two_source_manifest_failures(tmp_path):
    batch = tmp_path / "batch"
    shutil.copytree(SAMPLE_BATCH_DIR, batch)
    target = batch / "historical_trips.jsonl"
    pristine = target.read_bytes()
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    clock = ticking_clock(T0)

    failed = []
    for tamper in (pristine[: len(pristine) // 2], pristine + b'{"extra": 1}\n'):
        target.write_bytes(tamper)
        with pytest.raises(SourceManifestError):
            ingest_batch(batch, run_id="b1", output_dir=out, clock=clock)
        failed.append(store.read_manifest("b1"))
    assert failed[1]["previous_attempts"] == [expected_entry(failed[0])]

    target.write_bytes(pristine)
    result = ingest_batch(batch, run_id="b1", output_dir=out, clock=clock)
    assert result.status == "COMMITTED"
    assert result.manifest["previous_attempts"] == [expected_entry(f) for f in failed]
    assert [h["error"]["type"] for h in result.manifest["previous_attempts"]] == ["SourceManifestError"] * 2
    assert accounting_identity_holds(result.manifest["totals"])
    clean = ingest_batch(SAMPLE_BATCH_DIR, run_id="b1", output_dir=tmp_path / "clean", clock=same_time_as(result))
    assert result.manifest["totals"] == clean.manifest["totals"]
    assert _data_files(out) == _data_files(tmp_path / "clean")


def test_refused_duplicate_attempt_is_recorded_with_null_totals(tmp_path):
    """A refusal before any line is counted still leaves an audit entry."""
    lines = clean_stream_lines(4)
    a = write_lines(tmp_path / "a.ndjson", lines)
    copy = write_lines(tmp_path / "copy.ndjson", lines)
    out = tmp_path / "bronze"
    with pytest.raises(DuplicateInputError):
        ingest_stream([a, copy], run_id="r1", output_dir=out, clock=ticking_clock(T0))
    failed = BronzeStore(out).read_manifest("r1")
    result = ingest_stream([a], run_id="r1", output_dir=out, clock=ticking_clock(T0.replace(hour=9)))
    assert result.manifest["previous_attempts"] == [expected_entry(failed)]
    assert result.manifest["previous_attempts"][0]["totals"] is None


def test_crash_after_failed_attempt_keeps_the_failed_entry(tmp_path, monkeypatch):
    """A crashed attempt writes no manifest, so it cannot be listed; the
    FAILED attempt before it is still kept, and the crash's leftovers are
    recorded in `removed_uncommitted_paths`."""
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(5))
    clock = ticking_clock(T0)
    _fail_on_publish(monkeypatch)
    with pytest.raises(OSError):
        ingest_stream([src], run_id="r1", output_dir=out, clock=clock)
    monkeypatch.undo()
    failed = store.read_manifest("r1")
    # Simulated crash of a second attempt: orphan partition + staging.
    orphan = store.partition_file(TABLE, "r1")
    orphan.parent.mkdir(parents=True)
    orphan.write_text('{"garbage": true}\n')
    (store.staging_dir("r1") / TABLE).mkdir(parents=True)

    result = ingest_stream([src], run_id="r1", output_dir=out, clock=clock)
    assert result.manifest["previous_attempts"] == [expected_entry(failed)]
    assert sorted(result.manifest["removed_uncommitted_paths"]) == ["_staging/r1", f"{TABLE}/run_id=r1"]


def _summary(result):
    assert result.stdout, result.stderr.decode()
    return json.loads(result.stdout.decode().strip().splitlines()[-1])


def test_cli_two_failures_then_success_then_noop(tmp_path):
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    lines = clean_stream_lines(8)
    good = write_lines(tmp_path / "events.ndjson", lines)
    truncated = tmp_path / "truncated.ndjson"
    truncated.write_bytes(("\n".join(lines) + '\n{"event_id": "E').encode())
    copy = write_lines(tmp_path / "copy.ndjson", lines)
    common = ("--run-id", "c1", "--output-dir", str(out))

    first = run_cli("stream", "--input", str(truncated), "--strict", *common,
                    "--fixed-ingestion-time", "2025-03-01T08:00:00Z")
    assert first.returncode == 1
    assert _summary(first)["error_type"] == "MalformedInputError"
    failed1 = store.read_manifest("c1")

    second = run_cli("stream", "--input", str(good), "--input", str(copy), *common,
                     "--fixed-ingestion-time", "2025-03-01T08:05:00Z")
    assert second.returncode == 1
    assert _summary(second)["error_type"] == "DuplicateInputError"
    failed2 = store.read_manifest("c1")
    assert failed2["previous_attempts"] == [expected_entry(failed1)]

    third = run_cli("stream", "--input", str(good), *common, "--fixed-ingestion-time", "2025-03-01T08:10:00Z")
    assert third.returncode == 0, third.stderr.decode()
    assert _summary(third)["status"] == "COMMITTED"
    committed = store.read_manifest("c1")
    assert committed["previous_attempts"] == [expected_entry(failed1), expected_entry(failed2)]
    assert [h["started_at"] for h in committed["previous_attempts"]] == [
        "2025-03-01T08:00:00.000000Z",
        "2025-03-01T08:05:00.000000Z",
    ]
    assert accounting_identity_holds(committed["totals"])
    assert committed["totals"]["records_written"] == len(lines)
    before = store.manifest_path("c1").read_bytes()

    fourth = run_cli("stream", "--input", str(good), *common, "--fixed-ingestion-time", "2025-03-01T09:00:00Z")
    assert fourth.returncode == 0
    assert _summary(fourth)["status"] == "ALREADY_COMMITTED"
    assert store.manifest_path("c1").read_bytes() == before
    assert len(store.read_manifest("c1")["previous_attempts"]) == 2
