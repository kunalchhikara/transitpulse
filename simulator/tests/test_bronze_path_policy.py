"""DE-09 criterion 2 (BACKLOG FU-04): called with absolute input paths, no
field of any Bronze manifest - COMMITTED or FAILED, including
`previous_attempts` and error messages - starts with `/` or contains the home
directory (or any absolute input/output root).

Path policy (README "DE-05: Run manifest"): `input_files[].path` is the file
name relative to its input directory, i.e. the basename (`-` for stdin), and
the manifest declares it as `input_path_policy = "basename"`. Store-internal
paths (`tables[].path`, `removed_uncommitted_paths`) are relative to the
Bronze root. Store and input roots are never recorded; any absolute root that
reaches an error message is replaced by a placeholder.

`HOME` is pointed at the test's temporary directory where possible so the
"contains the home directory" check is exercised for real, not vacuously.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from simulator.bronze import (
    BronzeStore,
    MalformedInputError,
    ParseErrorPolicy,
    SourceManifestError,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    REPO_ROOT,
    SAMPLE_BATCH_DIR,
    clean_stream_lines,
    path_leaks,
    run_cli,
    write_lines,
)

TABLE = load_bronze_config().stream.table
REAL_HOME = Path.home()


def _roots(tmp_path: Path) -> tuple:
    return (tmp_path, tmp_path.resolve(), Path.home(), REAL_HOME, REPO_ROOT)


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    """HOME == a fresh directory holding every input and output of the test."""
    root = tmp_path / "home" / "legacy-user"
    root.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(root))
    assert Path.home() == root
    return root


def _all_manifests(out: Path) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted((out / "_runs").glob("*.json"))]


def test_stream_absolute_inputs_record_no_absolute_path(home, tmp_path):
    lines = clean_stream_lines(10)
    (home / "data" / "in").mkdir(parents=True)
    a = write_lines(home / "data" / "in" / "a.ndjson", lines[:5])
    b = write_lines(home / "data" / "in" / "b.ndjson", lines[5:])
    assert a.is_absolute() and str(home) in str(a)
    out = home / "bronze"
    result = ingest_stream([a, b], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    m = result.manifest
    assert [f["path"] for f in m["input_files"]] == ["a.ndjson", "b.ndjson"]
    assert m["input_path_policy"] == "basename"
    assert path_leaks(m, *_roots(tmp_path)) == []
    for on_disk in _all_manifests(out):
        assert path_leaks(on_disk, *_roots(tmp_path)) == []


def test_batch_absolute_input_dir_records_no_absolute_path(home, tmp_path):
    batch = home / "batch"
    shutil.copytree(SAMPLE_BATCH_DIR, batch)
    out = home / "bronze"
    m = ingest_batch(batch.resolve(), run_id="b1", output_dir=out.resolve(), clock=FIXED_CLOCK).manifest
    assert {f["path"] for f in m["input_files"]} == {f["file"] for f in m["input_files"]}
    assert all("/" not in f["path"] for f in m["input_files"])
    assert m["source_reconciliation"]["source_manifest"] == "manifest.json"
    assert path_leaks(m, *_roots(tmp_path), batch, SAMPLE_BATCH_DIR) == []


def test_committed_sample_under_repo_root_records_no_repo_path(tmp_path):
    """The repository's own absolute path (under the real home on a dev
    machine, `/home/runner/...` on CI) never reaches the manifest."""
    assert SAMPLE_BATCH_DIR.is_absolute()
    m = ingest_batch(SAMPLE_BATCH_DIR, run_id="b1", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK).manifest
    assert path_leaks(m, *_roots(tmp_path), SAMPLE_BATCH_DIR) == []


def test_relative_inputs_are_recorded_as_basename_too(tmp_path, monkeypatch):
    (tmp_path / "nested" / "dir").mkdir(parents=True)
    write_lines(tmp_path / "nested" / "dir" / "e.ndjson", clean_stream_lines(3))
    monkeypatch.chdir(tmp_path)
    m = ingest_stream(["nested/dir/e.ndjson"], run_id="r1", output_dir="bronze", clock=FIXED_CLOCK).manifest
    assert m["input_files"][0]["path"] == "e.ndjson"


def test_failed_manifest_error_with_absolute_path_is_redacted(home, tmp_path, monkeypatch):
    src = write_lines(home / "e.ndjson", clean_stream_lines(4))
    out = home / "bronze"

    def denied(self, table, run_id):
        raise PermissionError(13, "Permission denied", str(self.partition_dir(table, run_id).resolve()))

    monkeypatch.setattr(BronzeStore, "publish_table", denied)
    with pytest.raises(PermissionError) as raised:
        ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    # The caller still gets the full, unredacted exception ...
    assert str(home) in str(raised.value)
    # ... but the persisted manifest does not.
    failed = BronzeStore(out).read_manifest("r1")
    assert failed["status"] == "FAILED"
    assert path_leaks(failed, *_roots(tmp_path)) == []
    assert f"{TABLE}/run_id=r1" in failed["error"]["message"]  # still useful

    monkeypatch.undo()
    monkeypatch.setenv("HOME", str(home))
    retried = ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    assert retried.status == "COMMITTED"
    assert len(retried.manifest["previous_attempts"]) == 1
    assert path_leaks(retried.manifest, *_roots(tmp_path)) == []


def test_source_manifest_mismatch_manifest_is_path_free(home, tmp_path):
    batch = home / "batch"
    shutil.copytree(SAMPLE_BATCH_DIR, batch)
    (batch / "historical_trips.jsonl").write_bytes(b'{"tampered": true}\n')
    out = home / "bronze"
    with pytest.raises(SourceManifestError):
        ingest_batch(batch, run_id="b1", output_dir=out, clock=FIXED_CLOCK)
    assert path_leaks(BronzeStore(out).read_manifest("b1"), *_roots(tmp_path), batch) == []


def test_strict_failure_manifest_is_path_free(home, tmp_path):
    src = home / "bad.ndjson"
    src.write_bytes(b'{"event_id": "E1"}\nnot json\n')
    strict = load_bronze_config().model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
    out = home / "bronze"
    with pytest.raises(MalformedInputError):
        ingest_stream([src], run_id="r1", output_dir=out, clock=FIXED_CLOCK, config=strict)
    assert path_leaks(BronzeStore(out).read_manifest("r1"), *_roots(tmp_path)) == []


# -- CLI (separate processes) --------------------------------------------------


def _env(home: Path) -> dict[str, str]:
    return {**os.environ, "HOME": str(home)}


def _log_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_cli_stream_absolute_paths(home, tmp_path):
    lines = clean_stream_lines(6)
    (home / "in").mkdir()
    a = write_lines(home / "in" / "a.ndjson", lines)
    out = home / "bronze"
    log = home / "bronze.log"
    result = run_cli("stream", "--input", str(a.resolve()), "--run-id", "c1", "--output-dir", str(out.resolve()),
                     "--fixed-ingestion-time", "2025-02-01T09:30:00Z", "--log-file", str(log), env=_env(home))
    assert result.returncode == 0, result.stderr.decode()
    m = BronzeStore(out).read_manifest("c1")
    assert m["input_files"][0]["path"] == "a.ndjson"
    assert path_leaks(m, *_roots(tmp_path)) == []
    # Structured logs carry the store-relative manifest location, not a path.
    for record in _log_lines(log):
        assert path_leaks(record.get("context", {}), *_roots(tmp_path)) == [], record


def test_cli_batch_absolute_input_dir(home, tmp_path):
    batch = home / "batch"
    shutil.copytree(SAMPLE_BATCH_DIR, batch)
    out = home / "bronze"
    result = run_cli("batch", "--input-dir", str(batch.resolve()), "--run-id", "b1", "--output-dir", str(out),
                     "--fixed-ingestion-time", "2025-02-01T09:30:00Z", env=_env(home))
    assert result.returncode == 0, result.stderr.decode()
    assert path_leaks(BronzeStore(out).read_manifest("b1"), *_roots(tmp_path), batch) == []

    # A FAILED attempt, then its successful retry, through the CLI.
    (batch / "historical_trips.jsonl").write_bytes(b'{"tampered": true}\n')
    failed = run_cli("batch", "--input-dir", str(batch.resolve()), "--run-id", "b2", "--output-dir", str(out),
                     "--fixed-ingestion-time", "2025-02-01T09:31:00Z", env=_env(home))
    assert failed.returncode == 1
    assert path_leaks(BronzeStore(out).read_manifest("b2"), *_roots(tmp_path), batch) == []


def test_cli_manifests_byte_identical_from_different_absolute_locations(tmp_path):
    """Same content + run_id + fixed clock from two different absolute input
    directories -> byte-identical manifests (the location is not recorded)."""
    lines = clean_stream_lines(6)
    manifests = []
    for where in ("first/location", "somewhere/else/entirely"):
        (tmp_path / where).mkdir(parents=True)
        src = write_lines((tmp_path / where).resolve() / "events.ndjson", lines)
        out = tmp_path / where / "bronze"
        result = run_cli("stream", "--input", str(src), "--run-id", "same", "--output-dir", str(out),
                         "--fixed-ingestion-time", "2025-02-01T09:30:00Z")
        assert result.returncode == 0, result.stderr.decode()
        manifests.append((out / "_runs" / "same.json").read_bytes())
    assert manifests[0] == manifests[1]
