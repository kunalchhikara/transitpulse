"""DE-05 idempotency / replay policy (README "DE-05: Idempotency and replay"):

* same run_id + same input (committed)    -> no-op, output byte-identical
* same run_id + different input (committed) -> RunIdConflictError, untouched
* same run_id after a FAILED / crashed attempt -> clean retry
* new run_id + already-ingested input      -> DuplicateInputError (no silent
  double-ingestion) unless allow_replay, which commits and flags `replay_of`
* new run_id + new input                   -> appended as a new partition
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

from simulator.bronze import (
    BronzeStore,
    DuplicateInputError,
    InvalidRunIdError,
    MalformedInputError,
    ParseErrorPolicy,
    RunIdConflictError,
    fixed_clock,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    SAMPLE_BATCH_DIR,
    clean_stream_lines,
    write_lines,
)

TABLE = load_bronze_config().stream.table
LATER_CLOCK = fixed_clock(datetime(2025, 3, 1, tzinfo=UTC))


def _snapshot(root):
    return {
        p.relative_to(root).as_posix(): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def test_same_run_id_same_input_is_a_noop_and_output_is_byte_identical(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(20))
    out = tmp_path / "bronze"
    first = ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    before = _snapshot(out)
    # A later wall-clock time must not leak in: the rerun writes nothing.
    second = ingest_stream([src], run_id="r1", output_dir=out, clock=LATER_CLOCK)
    assert first.status == "COMMITTED"
    assert second.status == "ALREADY_COMMITTED"
    assert second.manifest == first.manifest
    assert _snapshot(out) == before
    assert len(list(BronzeStore(out).read_table(TABLE))) == 20


def test_same_run_id_same_batch_input_is_a_noop(tmp_path):
    out = tmp_path / "bronze"
    ingest_batch(SAMPLE_BATCH_DIR, run_id="b1", output_dir=out, clock=FIXED_CLOCK)
    before = _snapshot(out)
    again = ingest_batch(SAMPLE_BATCH_DIR, run_id="b1", output_dir=out, clock=LATER_CLOCK)
    assert again.status == "ALREADY_COMMITTED"
    assert _snapshot(out) == before


def test_same_run_id_different_input_is_refused_and_committed_run_untouched(tmp_path):
    lines = clean_stream_lines(20)
    a = write_lines(tmp_path / "e.ndjson", lines[:10])
    out = tmp_path / "bronze"
    ingest_stream([a], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    before = _snapshot(out)
    b = write_lines(tmp_path / "other.ndjson", lines[10:])
    with pytest.raises(RunIdConflictError, match="r1"):
        ingest_stream([b], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    assert _snapshot(out) == before


def test_new_run_id_with_already_ingested_input_is_refused(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(10))
    out = tmp_path / "bronze"
    ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    # Same bytes under a different file name are still the same input.
    copy = write_lines(tmp_path / "renamed.ndjson", clean_stream_lines(10))
    with pytest.raises(DuplicateInputError, match="r1"):
        ingest_stream([copy], run_id="r2", output_dir=out, clock=FIXED_CLOCK)
    store = BronzeStore(out)
    assert store.read_manifest("r2")["status"] == "FAILED"
    assert store.read_manifest("r2")["error"]["type"] == "DuplicateInputError"
    assert not store.partition_dir(TABLE, "r2").exists()
    assert len(list(store.read_table(TABLE))) == 10  # no double-ingestion


def test_new_run_id_partial_overlap_of_input_files_is_refused(tmp_path):
    lines = clean_stream_lines(20)
    a = write_lines(tmp_path / "a.ndjson", lines[:10])
    b = write_lines(tmp_path / "b.ndjson", lines[10:])
    out = tmp_path / "bronze"
    ingest_stream([a], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    with pytest.raises(DuplicateInputError, match="a.ndjson"):
        ingest_stream([a, b], run_id="r2", output_dir=out, clock=FIXED_CLOCK)


def test_same_content_twice_within_one_run_is_refused_unless_replay_allowed(tmp_path):
    lines = clean_stream_lines(5)
    a = write_lines(tmp_path / "a.ndjson", lines)
    copy = write_lines(tmp_path / "copy.ndjson", lines)
    out = tmp_path / "bronze"
    with pytest.raises(DuplicateInputError, match="more than once"):
        ingest_stream([a, copy], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    assert BronzeStore(out).committed_manifests() == []
    result = ingest_stream([a, copy], run_id="r2", output_dir=out, clock=FIXED_CLOCK, allow_replay=True)
    assert result.manifest["duplicate_input_files"] == [["a.ndjson", "copy.ndjson"]]
    assert len(list(BronzeStore(out).read_table(TABLE))) == 10


def test_allow_replay_commits_and_flags_the_replay(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(10))
    out = tmp_path / "bronze"
    ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    replay = ingest_stream([src], run_id="r2", output_dir=out, clock=LATER_CLOCK, allow_replay=True)
    assert replay.status == "COMMITTED"
    assert replay.manifest["replay_of"] == ["r1"]
    assert replay.manifest["allow_replay"] is True
    records = list(BronzeStore(out).read_table(TABLE))
    assert len(records) == 20
    # The two copies are distinguishable by run_id/ingestion_time.
    assert [r["run_id"] for r in records] == ["r1"] * 10 + ["r2"] * 10


def test_new_run_id_new_input_appends_a_partition(tmp_path):
    lines = clean_stream_lines(20)
    out = tmp_path / "bronze"
    ingest_stream([write_lines(tmp_path / "a.ndjson", lines[:10])], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    second = ingest_stream(
        [write_lines(tmp_path / "b.ndjson", lines[10:])], run_id="r2", output_dir=out, clock=LATER_CLOCK
    )
    assert second.manifest["replay_of"] == []
    store = BronzeStore(out)
    assert [m["run_id"] for m in store.committed_manifests()] == ["r1", "r2"]
    assert len(list(store.read_table(TABLE))) == 20


def test_empty_input_files_never_count_as_replays(tmp_path):
    """DE-04 writes an empty weather file when weather is disabled; an empty
    file cannot cause double-ingestion and must not block a later run."""
    out = tmp_path / "bronze"
    empty = tmp_path / "empty.ndjson"
    empty.write_bytes(b"")
    ingest_stream([empty], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    result = ingest_stream([empty], run_id="r2", output_dir=out, clock=FIXED_CLOCK)
    assert result.status == "COMMITTED"
    assert result.manifest["replay_of"] == []


def test_retry_after_failed_attempt_with_same_run_id_succeeds(tmp_path):
    out = tmp_path / "bronze"
    bad = tmp_path / "e.ndjson"
    bad.write_bytes(b'{"event_id": "E1"}\n{"trunc')
    strict = load_bronze_config().model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
    with pytest.raises(MalformedInputError):
        ingest_stream([bad], run_id="r1", output_dir=out, clock=FIXED_CLOCK, config=strict)
    store = BronzeStore(out)
    assert store.read_manifest("r1")["status"] == "FAILED"
    assert not (out / "_staging").exists()
    # Fix the input and retry under the same run_id.
    bad.write_bytes(b'{"event_id": "E1"}\n')
    result = ingest_stream([bad], run_id="r1", output_dir=out, clock=FIXED_CLOCK, config=strict)
    assert result.status == "COMMITTED"
    assert result.manifest["error"] is None


def test_crashed_attempt_leftovers_are_invisible_and_cleaned_on_retry(tmp_path):
    """Simulate a crash after a partition was published but before the
    manifest (commit marker) was written."""
    lines = clean_stream_lines(10)
    src = write_lines(tmp_path / "e.ndjson", lines)
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    orphan = store.partition_file(TABLE, "r1")
    orphan.parent.mkdir(parents=True)
    orphan.write_text('{"garbage": true}\n')
    staging = store.staging_dir("r1") / TABLE
    staging.mkdir(parents=True)
    (staging / "part-00000.jsonl").write_text("half-written")
    assert list(store.read_table(TABLE)) == []  # uncommitted data is never read

    result = ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    assert sorted(result.manifest["removed_uncommitted_paths"]) == sorted(
        ["_staging/r1", f"{TABLE}/run_id=r1"]
    )
    records = list(store.read_table(TABLE))
    assert len(records) == 10
    assert "garbage" not in records[0]
    assert not (out / "_staging").exists()


def test_failure_during_commit_leaves_nothing_visible(tmp_path, monkeypatch):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(10))
    out = tmp_path / "bronze"
    store_cls = BronzeStore
    original = store_cls.publish_table

    def boom(self, table, run_id):
        original(self, table, run_id)
        raise OSError("disk full")

    monkeypatch.setattr(store_cls, "publish_table", boom)
    with pytest.raises(OSError, match="disk full"):
        ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    monkeypatch.setattr(store_cls, "publish_table", original)
    store = BronzeStore(out)
    assert store.read_manifest("r1")["status"] == "FAILED"
    assert store.committed_manifests() == []
    assert list(store.read_table(TABLE)) == []
    # ... and the retry recovers.
    assert ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK).status == "COMMITTED"
    assert len(list(store.read_table(TABLE))) == 10


@pytest.mark.parametrize("bad", ["", "../escape", "a/b", ".hidden", "x" * 200, "white space"])
def test_unsafe_run_ids_rejected(tmp_path, bad):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(1))
    with pytest.raises(InvalidRunIdError):
        ingest_stream([src], run_id=bad, output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)
    assert not os.path.exists(tmp_path / "escape")
