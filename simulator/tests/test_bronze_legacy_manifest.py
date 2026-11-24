"""DE-09 criterion 3: Bronze manifests written before DE-09 (no
`previous_attempts`, no `input_path_policy`, `input_files[].path` recorded
exactly as given - typically an absolute local path) are still accepted by
`BronzeStore`, by Bronze's own idempotency/replay guard, by DE-06 Silver and,
through Silver, by DE-07 Gold.

Two kinds of fixture:

* `LEGACY_FAILED_MANIFEST` - a verbatim FAILED manifest produced by the
  pre-DE-09 code (main @ 295f7db; only the absolute temp-dir prefix of
  `input_files[].path` was replaced with a synthetic `/home/legacy-user/...`).
* `to_legacy(...)` - rewrites a freshly committed manifest into the pre-DE-09
  shape (needed for COMMITTED fixtures, whose sha256/record counts must match
  real data files). The result is checked against `LEGACY_*_KEYS`, the exact
  key sets the pre-DE-09 code wrote, so the fixture is provably legacy-shaped.

Silver and Gold are only imported and run here; their code and tests are not
touched by DE-09.
"""

from __future__ import annotations

import copy
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from simulator.bronze import (
    BronzeStore,
    DuplicateInputError,
    fixed_clock,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.bronze.store import dump_manifest
from simulator.gold import run_gold
from simulator.silver import run_silver
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    SAMPLE_BATCH_DIR,
    clean_stream_lines,
    path_leaks,
    write_lines,
)

TABLE = load_bronze_config().stream.table
SILVER_CLOCK = fixed_clock(datetime(2025, 2, 1, 10, 0, 0, tzinfo=UTC))
GOLD_CLOCK = fixed_clock(datetime(2025, 2, 1, 11, 0, 0, tzinfo=UTC))
LEGACY_INPUT_ROOT = "/home/legacy-user/inputs"

#: Captured from the pre-DE-09 code (main @ 295f7db).
LEGACY_TOP_LEVEL_KEYS = frozenset({
    "accounting_identity", "accounting_identity_holds", "allow_replay", "bronze_manifest_version",
    "duplicate_input_files", "duration_seconds", "ended_at", "error", "ignored_files", "ingestion_time",
    "input_files", "input_fingerprint", "mode", "parse_error_policy", "removed_uncommitted_paths",
    "replay_of", "run_id", "source", "source_reconciliation", "started_at", "status", "tables", "totals",
})
LEGACY_INPUT_FILE_KEYS = frozenset({
    "blank_lines_skipped", "file", "index", "json_object_records", "lines_read", "parse_error_records",
    "parse_errors_by_code", "path", "records_read", "records_written", "sha256", "size_bytes", "source",
    "table", "unterminated_final_line",
})
LEGACY_TABLE_KEYS = frozenset({
    "blank_lines_skipped", "input_files", "json_object_records", "lines_read", "parse_error_records",
    "path", "records_read", "records_written", "sha256", "source",
})
#: Fields DE-09 adds (the only difference from the legacy shape).
DE09_FIELDS = ("previous_attempts", "input_path_policy")

LEGACY_FAILED_MANIFEST: dict[str, Any] = {
    "accounting_identity": (
        "lines_read == records_read + blank_lines_skipped; records_read == records_written; "
        "records_written == json_object_records + parse_error_records"
    ),
    "accounting_identity_holds": None,
    "allow_replay": False,
    "bronze_manifest_version": "1.0.0",
    "duplicate_input_files": [["events.ndjson", "copy.ndjson"]],
    "duration_seconds": 0.0,
    "ended_at": "2025-02-01T09:30:00.000000Z",
    "error": {
        "message": (
            "identical input content given more than once in this run: [['events.ndjson', 'copy.ndjson']]. "
            "Refusing silent double-ingestion; pass allow_replay/--allow-replay to replay deliberately"
        ),
        "type": "DuplicateInputError",
    },
    "ignored_files": [],
    "ingestion_time": "2025-02-01T09:30:00.000000Z",
    "input_files": [
        {
            "file": "events.ndjson", "index": 0, "path": f"{LEGACY_INPUT_ROOT}/events.ndjson",
            "sha256": "259e2dd40995205a64a5de9058babec70f53b1bb167b65cf90bad716444b3de3", "size_bytes": 1115,
            "source": "simulator.telemetry_stream", "table": "telemetry_stream",
        },
        {
            "file": "copy.ndjson", "index": 1, "path": f"{LEGACY_INPUT_ROOT}/copy.ndjson",
            "sha256": "259e2dd40995205a64a5de9058babec70f53b1bb167b65cf90bad716444b3de3", "size_bytes": 1115,
            "source": "simulator.telemetry_stream", "table": "telemetry_stream",
        },
    ],
    "input_fingerprint": "dffe13cd8d5c1dc18bbb365c86e545dfaafdbee5faf2a61c28c21125d973b86d",
    "mode": "stream",
    "parse_error_policy": "preserve",
    "removed_uncommitted_paths": [],
    "replay_of": [],
    "run_id": "legacy-failed",
    "source": "simulator.telemetry_stream",
    "source_reconciliation": None,
    "started_at": "2025-02-01T09:30:00.000000Z",
    "status": "FAILED",
    "tables": {},
    "totals": None,
}


def to_legacy(manifest: dict[str, Any]) -> dict[str, Any]:
    legacy = copy.deepcopy(manifest)
    for key in DE09_FIELDS:
        legacy.pop(key, None)
    for entry in legacy["input_files"]:
        if entry["path"] != "-":
            entry["path"] = f"{LEGACY_INPUT_ROOT}/{entry['file']}"
    assert set(legacy) == LEGACY_TOP_LEVEL_KEYS
    for entry in legacy["input_files"]:
        assert set(entry) - {"source_manifest_check"} == LEGACY_INPUT_FILE_KEYS
    for table in legacy["tables"].values():
        assert set(table) == LEGACY_TABLE_KEYS
    return legacy


def rewrite_as_legacy(store_root: Path, run_id: str) -> dict[str, Any]:
    store = BronzeStore(store_root)
    legacy = to_legacy(store.read_manifest(run_id))
    store.manifest_path(run_id).write_text(dump_manifest(legacy), encoding="utf-8")
    return legacy


def _files(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_failed_fixture_is_pre_de09_shaped():
    assert set(LEGACY_FAILED_MANIFEST) == LEGACY_TOP_LEVEL_KEYS
    assert not set(DE09_FIELDS) & set(LEGACY_FAILED_MANIFEST)
    for entry in LEGACY_FAILED_MANIFEST["input_files"]:
        assert entry["path"].startswith("/")


def test_retry_over_legacy_failed_manifest(tmp_path):
    out = tmp_path / "bronze"
    store = BronzeStore(out)
    store.write_manifest(copy.deepcopy(LEGACY_FAILED_MANIFEST))
    assert store.committed_manifests() == []  # a FAILED legacy run is never visible
    src = write_lines(tmp_path / "events.ndjson", clean_stream_lines(3))

    result = ingest_stream([src], run_id="legacy-failed", output_dir=out,
                           clock=fixed_clock(datetime(2025, 2, 2, tzinfo=UTC)))
    assert result.status == "COMMITTED"
    assert result.manifest["previous_attempts"] == [
        {
            "status": "FAILED",
            "started_at": "2025-02-01T09:30:00.000000Z",
            "ended_at": "2025-02-01T09:30:00.000000Z",
            "duration_seconds": 0.0,
            "input_fingerprint": LEGACY_FAILED_MANIFEST["input_fingerprint"],
            "error": LEGACY_FAILED_MANIFEST["error"],
            "totals": None,
        }
    ]
    # The legacy absolute input paths are not carried into the new manifest.
    assert path_leaks(result.manifest, LEGACY_INPUT_ROOT, tmp_path) == []


@pytest.mark.parametrize("mode", ["stream", "batch"])
def test_bronze_store_and_guards_accept_legacy_committed_manifest(tmp_path, mode):
    out = tmp_path / "bronze"
    if mode == "stream":
        src = write_lines(tmp_path / "events.ndjson", clean_stream_lines(10))

        def ingest(run_id, **kw):
            return ingest_stream([src], run_id=run_id, output_dir=out, **kw)
    else:
        def ingest(run_id, **kw):
            return ingest_batch(SAMPLE_BATCH_DIR, run_id=run_id, output_dir=out, **kw)

    committed = ingest("legacy", clock=FIXED_CLOCK)
    store = BronzeStore(out)
    records_before = {t: list(store.read_table(t)) for t in committed.manifest["tables"]}
    legacy = rewrite_as_legacy(out, "legacy")
    legacy_bytes = store.manifest_path("legacy").read_bytes()

    # BronzeStore reads committed data through the legacy manifest unchanged.
    assert [m["run_id"] for m in store.committed_manifests()] == ["legacy"]
    assert {t: list(store.read_table(t)) for t in legacy["tables"]} == records_before

    # Same run_id + same input -> no-op; the legacy manifest is left untouched.
    again = ingest("legacy", clock=fixed_clock(datetime(2026, 1, 1, tzinfo=UTC)))
    assert again.status == "ALREADY_COMMITTED"
    assert store.manifest_path("legacy").read_bytes() == legacy_bytes

    # The replay guard still recognises content the legacy run ingested.
    with pytest.raises(DuplicateInputError):
        ingest("new-run", clock=FIXED_CLOCK)
    replay = ingest("replay-run", clock=FIXED_CLOCK, allow_replay=True)
    assert replay.manifest["replay_of"] == ["legacy"]
    # new-run's FAILED manifest (written by the new code) is path-free.
    assert path_leaks(store.read_manifest("new-run"), tmp_path, SAMPLE_BATCH_DIR) == []
    assert store.manifest_path("legacy").read_bytes() == legacy_bytes


def test_silver_and_gold_accept_legacy_bronze_manifest(tmp_path):
    """Silver and Gold over a Bronze store whose committed manifest is
    pre-DE-09-shaped produce byte-identical output to the same pipeline over
    a DE-09 manifest of the same data."""
    lines = clean_stream_lines(40)
    src = write_lines(tmp_path / "events.ndjson", lines)
    new_bronze, legacy_bronze = tmp_path / "new" / "bronze", tmp_path / "legacy" / "bronze"
    ingest_stream([src], run_id="b1", output_dir=new_bronze, clock=FIXED_CLOCK)
    shutil.copytree(new_bronze, legacy_bronze)
    rewrite_as_legacy(legacy_bronze, "b1")
    assert _files(new_bronze)["_runs/b1.json"] != _files(legacy_bronze)["_runs/b1.json"]

    outputs = {}
    for name, bronze in (("new", new_bronze), ("legacy", legacy_bronze)):
        silver = run_silver(run_id="s1", bronze_dir=bronze, output_dir=bronze.parent / "silver", clock=SILVER_CLOCK)
        assert silver.status == "COMMITTED"
        assert silver.manifest["reconciliation"]["holds"] is True
        assert silver.manifest["counters"]["extracted"] == len(lines)
        gold = run_gold(run_id="g1", silver_dir=bronze.parent / "silver", output_dir=bronze.parent / "gold",
                        clock=GOLD_CLOCK)
        assert gold.status == "COMMITTED"
        assert gold.manifest["reconciliation"]["holds"] is True
        outputs[name] = (_files(bronze.parent / "silver"), _files(bronze.parent / "gold"))
    assert outputs["legacy"] == outputs["new"]
