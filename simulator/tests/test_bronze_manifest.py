"""DE-05 per-run audit manifest and its accounting identity:

    lines_read      == records_read + blank_lines_skipped
    records_read    == records_written
    records_written == json_object_records + parse_error_records

checked per input file, per table and in total, and independently re-derived
here from the input bytes and the data actually on disk."""

from __future__ import annotations

import hashlib
import json

from simulator.bronze import (
    BronzeStore,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.bronze.ingest import accounting_identity_holds
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    FIXED_TIME_STR,
    SAMPLE_BATCH_DIR,
    incident_stream_lines,
    write_lines,
)

TABLE = load_bronze_config().stream.table


def _messy_stream(tmp_path):
    lines = incident_stream_lines(60)
    messy = lines[:20] + ["", "   ", "{bad json", "[]"] + lines[20:] + ['{"cut": "of']
    path = tmp_path / "events.ndjson"
    write_lines(path, messy, trailing_newline=False)
    return path, messy


def test_manifest_accounting_identity_matches_independent_counts(tmp_path):
    path, messy = _messy_stream(tmp_path)
    out = tmp_path / "bronze"
    m = ingest_stream([path], run_id="acc", output_dir=out, clock=FIXED_CLOCK).manifest

    blank = sum(1 for line in messy if not line.strip())
    expected = {
        "lines_read": len(messy),
        "blank_lines_skipped": blank,
        "records_read": len(messy) - blank,
        "records_written": len(messy) - blank,
        "parse_error_records": 3,
        "json_object_records": len(messy) - blank - 3,
    }
    assert m["totals"] == expected
    for key, value in expected.items():
        assert m["tables"][TABLE][key] == value
        assert m["input_files"][0][key] == value
    assert accounting_identity_holds(m["totals"])
    assert m["accounting_identity_holds"] is True

    # On-disk evidence: the committed file has exactly records_written lines
    # and the sha256 the manifest claims.
    data = (out / m["tables"][TABLE]["path"]).read_bytes()
    assert data.count(b"\n") == m["totals"]["records_written"]
    assert hashlib.sha256(data).hexdigest() == m["tables"][TABLE]["sha256"]
    assert m["input_files"][0]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_manifest_records_run_audit_fields(tmp_path):
    path, _ = _messy_stream(tmp_path)
    m = ingest_stream([path], run_id="acc", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK).manifest
    assert m["run_id"] == "acc"
    assert m["status"] == "COMMITTED"
    assert m["mode"] == "stream"
    assert m["source"] == load_bronze_config().stream.source
    assert m["ingestion_time"] == m["started_at"] == m["ended_at"] == FIXED_TIME_STR
    assert m["duration_seconds"] == 0.0
    assert m["parse_error_policy"] == "preserve"
    assert [f["file"] for f in m["input_files"]] == ["events.ndjson"]
    assert m["error"] is None
    # The manifest on disk is what the API returned.
    on_disk = json.loads((tmp_path / "bronze" / "_runs" / "acc.json").read_text())
    assert on_disk == m


def test_batch_manifest_identity_per_table_and_total(tmp_path):
    m = ingest_batch(SAMPLE_BATCH_DIR, run_id="b", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK).manifest
    for table in m["tables"].values():
        assert accounting_identity_holds(table)
    for entry in m["input_files"]:
        assert accounting_identity_holds(entry)
        assert entry["source_manifest_check"]["status"] == "MATCHED"
    assert m["totals"]["records_written"] == sum(t["records_written"] for t in m["tables"].values())
    assert m["accounting_identity_holds"] is True
    store = BronzeStore(tmp_path / "bronze")
    for name, table in m["tables"].items():
        assert len(list(store.read_table(name))) == table["records_written"]


def test_identity_check_detects_violations():
    good = {"lines_read": 5, "blank_lines_skipped": 1, "records_read": 4, "records_written": 4,
            "json_object_records": 3, "parse_error_records": 1}
    assert accounting_identity_holds(good)
    assert not accounting_identity_holds(good | {"records_written": 3})
    assert not accounting_identity_holds(good | {"parse_error_records": 0})
    assert not accounting_identity_holds(good | {"blank_lines_skipped": 0})
