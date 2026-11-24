"""DE-05 streaming-shaped ingestion: DE-02 clean telemetry and a DE-03
incident-laced stream (every malformed mode, duplicates, late, out-of-order,
congestion, breakdown) land in Bronze exactly as received - one record per
input line, in arrival order, nothing validated, deduplicated or dropped."""

from __future__ import annotations

import io
import json
from collections import Counter
from datetime import datetime

from pydantic import ValidationError

from simulator.bronze import BronzeStore, ingest_stream, load_bronze_config
from simulator.bronze.ingest import STDIN_NAME
from simulator.schemas.telemetry import TelemetryEvent
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    assert_round_trip,
    clean_stream_lines,
    incident_stream_lines,
    incident_stream_payloads,
    read_jsonl,
    write_lines,
)

STREAM = load_bronze_config().stream


def _ingest(tmp_path, lines, name="events.ndjson", run_id="run-1"):
    src = write_lines(tmp_path / name, lines)
    out = tmp_path / "bronze"
    result = ingest_stream([src], run_id=run_id, output_dir=out, clock=FIXED_CLOCK)
    part = out / result.manifest["tables"][STREAM.table]["path"]
    return result, part


def test_clean_de02_stream_round_trips_every_field(tmp_path):
    lines = clean_stream_lines(150)
    result, part = _ingest(tmp_path, lines)
    assert result.status == "COMMITTED"
    assert_round_trip(
        lines,
        read_jsonl(part),
        part.read_text(encoding="utf-8").splitlines(),
        run_id="run-1",
        source=STREAM.source,
        source_file="events.ndjson",
    )
    assert result.manifest["totals"]["parse_error_records"] == 0


def test_incident_fixture_really_contains_every_anomaly():
    """Guard: the fixture below must actually exercise every DE-03 mode,
    otherwise the Bronze test would prove less than it claims."""
    payloads = incident_stream_payloads()
    assert any(p.get("vehicle_id", "x") is None for p in payloads)
    assert any(p.get("latitude") == 999.0 for p in payloads)
    assert any(p.get("speed_kmh") == -42.0 for p in payloads)
    assert any(p.get("event_time") == "not-a-timestamp" for p in payloads)
    assert any(p.get("route_id") == "ROUTE-UNKNOWN-9999" for p in payloads)
    assert any("occupancy" not in p for p in payloads)
    assert any(p.get("status") == "BREAKDOWN" for p in payloads)
    ids = [p["event_id"] for p in payloads]
    assert len(ids) != len(set(ids)), "no duplicate event_ids in fixture"
    late = [
        p
        for p in payloads
        if p["event_time"] != "not-a-timestamp"
        and datetime.fromisoformat(p["event_time"]) < datetime.fromisoformat(p["generated_at"])
    ]
    assert late, "no late events in fixture"
    generated = [datetime.fromisoformat(p["generated_at"]) for p in payloads]
    assert generated != sorted(generated), "no out-of-order delivery in fixture"
    invalid = 0
    for p in payloads:
        try:
            TelemetryEvent.model_validate(p)
        except ValidationError:
            invalid += 1
    assert invalid > 0


def test_incident_laced_stream_lands_in_bronze_exactly_as_received(tmp_path):
    lines = incident_stream_lines()
    result, part = _ingest(tmp_path, lines)
    records = read_jsonl(part)
    assert_round_trip(
        lines,
        records,
        part.read_text(encoding="utf-8").splitlines(),
        run_id="run-1",
        source=STREAM.source,
        source_file="events.ndjson",
    )
    # Schema-invalid but JSON-parseable payloads are NOT parse errors.
    assert result.manifest["totals"]["parse_error_records"] == 0
    # Duplicates are kept (dedup is Silver's job) ...
    in_counts = Counter(json.loads(line)["event_id"] for line in lines)
    out_counts = Counter(r["event_id"] for r in records)
    assert out_counts == in_counts
    assert max(out_counts.values()) > 1
    # ... and arrival order (incl. out-of-order delivery) is preserved.
    assert [r["event_id"] for r in records] == [json.loads(line)["event_id"] for line in lines]
    assert [r["_bronze_line_number"] for r in records] == list(range(1, len(lines) + 1))


def test_multiple_stream_files_ingested_in_order_with_per_file_lineage(tmp_path):
    lines = clean_stream_lines(30)
    a = write_lines(tmp_path / "a.ndjson", lines[:10])
    b = write_lines(tmp_path / "b.ndjson", lines[10:])
    out = tmp_path / "bronze"
    result = ingest_stream([a, b], run_id="multi", output_dir=out, clock=FIXED_CLOCK)
    records = list(BronzeStore(out).read_table(STREAM.table))
    assert [r["event_id"] for r in records] == [json.loads(line)["event_id"] for line in lines]
    assert [r["_bronze_source_file"] for r in records] == ["a.ndjson"] * 10 + ["b.ndjson"] * 20
    assert [r["_bronze_line_number"] for r in records] == list(range(1, 11)) + list(range(1, 21))
    assert [f["records_written"] for f in result.manifest["input_files"]] == [10, 20]


def test_stdin_input(tmp_path):
    lines = clean_stream_lines(5)
    data = ("\n".join(lines) + "\n").encode()
    out = tmp_path / "bronze"
    result = ingest_stream(["-"], run_id="stdin-run", output_dir=out, clock=FIXED_CLOCK, stdin=io.BytesIO(data))
    records = list(BronzeStore(out).read_table(STREAM.table))
    assert len(records) == 5
    assert {r["_bronze_source_file"] for r in records} == {STDIN_NAME}
    assert result.manifest["input_files"][0]["path"] == "-"


def test_source_label_override(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(3))
    out = tmp_path / "bronze"
    ingest_stream([src], run_id="s", output_dir=out, clock=FIXED_CLOCK, source="simulator.incidents_demo")
    assert {r["source"] for r in BronzeStore(out).read_table(STREAM.table)} == {"simulator.incidents_demo"}
