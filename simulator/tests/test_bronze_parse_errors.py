"""DE-05 truncated / unparseable input is never silently dropped.

Default policy (`preserve`): each such line becomes a marked parse-error
record holding the raw line, counted in the manifest. Strict policy (`fail`):
the run fails loudly, nothing is committed, a FAILED audit manifest records
why and where."""

from __future__ import annotations

import pytest

from simulator.bronze import (
    BronzeStore,
    MalformedInputError,
    ParseErrorPolicy,
    ingest_stream,
    load_bronze_config,
)
from simulator.bronze.records import raw_line_bytes
from simulator.tests._bronze_helpers import FIXED_CLOCK, clean_stream_lines

CONFIG = load_bronze_config()
TABLE = CONFIG.stream.table
STRICT = CONFIG.model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})


def _mixed_input(tmp_path):
    good = clean_stream_lines(4)
    raw = (
        good[0].encode() + b"\n"
        + b"garbage line\n"
        + good[1].encode() + b"\n"
        + b"\n"  # blank: no payload, counted, not stored
        + b"[1,2]\n"
        + b'{"a": "\xff"}\n'
        + good[2].encode() + b"\n"
        + good[3].encode()[:40]  # truncated final line, no newline
    )
    path = tmp_path / "events.ndjson"
    path.write_bytes(raw)
    return path, raw


def test_preserve_policy_keeps_every_unparseable_line_with_marker(tmp_path):
    path, raw = _mixed_input(tmp_path)
    out = tmp_path / "bronze"
    result = ingest_stream([path], run_id="r1", output_dir=out, clock=FIXED_CLOCK)
    records = list(BronzeStore(out).read_table(TABLE))

    assert len(records) == 7  # 8 physical lines - 1 blank
    errors = [r for r in records if "_bronze_parse_error" in r]
    assert [(r["_bronze_line_number"], r["_bronze_parse_error"]) for r in errors] == [
        (2, "invalid_json"),
        (5, "not_a_json_object"),
        (6, "invalid_utf8"),
        (8, "invalid_json"),
    ]
    physical = raw.split(b"\n")
    for record in errors:
        assert raw_line_bytes(record) == physical[record["_bronze_line_number"] - 1]
        assert record["run_id"] == "r1" and record["source"] == CONFIG.stream.source

    entry = result.manifest["input_files"][0]
    assert entry["unterminated_final_line"] is True
    assert entry["blank_lines_skipped"] == 1
    assert entry["parse_error_records"] == 4
    assert entry["parse_errors_by_code"]["invalid_json"] == 2
    assert entry["parse_errors_by_code"]["not_a_json_object"] == 1
    assert entry["parse_errors_by_code"]["invalid_utf8"] == 1
    assert result.manifest["totals"]["json_object_records"] == 3


def test_fail_policy_fails_loudly_and_commits_nothing(tmp_path):
    path, _ = _mixed_input(tmp_path)
    out = tmp_path / "bronze"
    with pytest.raises(MalformedInputError, match=r"4 unparseable line\(s\).*events.ndjson:2: invalid_json"):
        ingest_stream([path], run_id="r1", output_dir=out, clock=FIXED_CLOCK, config=STRICT)
    store = BronzeStore(out)
    manifest = store.read_manifest("r1")
    assert manifest["status"] == "FAILED"
    assert manifest["error"]["type"] == "MalformedInputError"
    assert manifest["totals"]["parse_error_records"] == 4  # still fully audited
    assert store.committed_manifests() == []
    assert list(store.read_table(TABLE)) == []
    assert not store.partition_dir(TABLE, "r1").exists()
    assert not (out / "_staging").exists()


def test_fail_policy_accepts_clean_input(tmp_path):
    path = tmp_path / "events.ndjson"
    path.write_text("\n".join(clean_stream_lines(5)) + "\n")
    result = ingest_stream([path], run_id="r1", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK, config=STRICT)
    assert result.status == "COMMITTED"


def test_truncated_final_object_line_that_still_parses_is_stored_but_flagged(tmp_path):
    """A final line without a newline is suspicious even when it parses; it
    is stored and the file is flagged `unterminated_final_line`."""
    path = tmp_path / "events.ndjson"
    lines = clean_stream_lines(3)
    path.write_text("\n".join(lines))  # no trailing newline
    result = ingest_stream([path], run_id="r1", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)
    entry = result.manifest["input_files"][0]
    assert entry["records_written"] == 3 and entry["parse_error_records"] == 0
    assert entry["unterminated_final_line"] is True


def test_crlf_line_endings_are_tolerated(tmp_path):
    path = tmp_path / "events.ndjson"
    lines = clean_stream_lines(3)
    path.write_bytes(("\r\n".join(lines) + "\r\n").encode())
    result = ingest_stream([path], run_id="r1", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)
    assert result.manifest["totals"]["json_object_records"] == 3
