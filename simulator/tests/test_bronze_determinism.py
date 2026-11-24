"""DE-05 determinism with an injected clock: same input + run_id + clock ->
byte-identical Bronze data and manifest, in-process and across separate OS
processes. Real runs stamp the wall clock (UTC)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from simulator.bronze import (
    fixed_clock,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.bronze.ingest import format_timestamp
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    SAMPLE_BATCH_DIR,
    incident_stream_lines,
    read_jsonl,
    run_cli,
    write_lines,
)

TABLE = load_bronze_config().stream.table


def _tree(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_injected_clock_gives_byte_identical_output_in_separate_stores(tmp_path):
    src = write_lines(tmp_path / "events.ndjson", incident_stream_lines(80))
    ingest_stream([src], run_id="det", output_dir=tmp_path / "a", clock=FIXED_CLOCK)
    ingest_stream([src], run_id="det", output_dir=tmp_path / "b", clock=FIXED_CLOCK)
    ingest_batch(SAMPLE_BATCH_DIR, run_id="detb", output_dir=tmp_path / "a", clock=FIXED_CLOCK)
    ingest_batch(SAMPLE_BATCH_DIR, run_id="detb", output_dir=tmp_path / "b", clock=FIXED_CLOCK)
    a, b = _tree(tmp_path / "a"), _tree(tmp_path / "b")
    assert a == b
    assert f"{TABLE}/run_id=det/part-00000.jsonl" in a


def test_cli_cross_process_determinism_with_fixed_ingestion_time(tmp_path):
    src = write_lines(tmp_path / "events.ndjson", incident_stream_lines(40))
    for name in ("a", "b"):
        result = run_cli(
            "stream", "--input", str(src), "--run-id", "x", "--output-dir", str(tmp_path / name),
            "--fixed-ingestion-time", "2025-02-01T09:30:00Z",
        )
        assert result.returncode == 0, result.stderr.decode()
    assert _tree(tmp_path / "a") == _tree(tmp_path / "b")
    records = read_jsonl(tmp_path / "a" / TABLE / "run_id=x" / "part-00000.jsonl")
    assert {r["ingestion_time"] for r in records} == {"2025-02-01T09:30:00.000000Z"}


def test_default_clock_is_wall_clock_utc(tmp_path):
    src = write_lines(tmp_path / "events.ndjson", incident_stream_lines(5))
    before = datetime.now(UTC)
    result = ingest_stream([src], run_id="live", output_dir=tmp_path / "bronze")
    after = datetime.now(UTC)
    stamped = datetime.fromisoformat(result.manifest["ingestion_time"])
    assert before <= stamped <= after
    assert result.manifest["ingestion_time"].endswith("Z")
    records = read_jsonl(tmp_path / "bronze" / result.manifest["tables"][TABLE]["path"])
    # One ingestion_time per run (Spark current_timestamp() semantics).
    assert {r["ingestion_time"] for r in records} == {result.manifest["ingestion_time"]}


def test_non_utc_clock_is_normalised_to_utc_and_naive_clock_rejected(tmp_path):
    src = write_lines(tmp_path / "events.ndjson", incident_stream_lines(2))
    sydney = timezone(timedelta(hours=10))
    clock = fixed_clock(datetime(2025, 2, 1, 19, 30, tzinfo=sydney))
    result = ingest_stream([src], run_id="tz", output_dir=tmp_path / "bronze", clock=clock)
    assert result.manifest["ingestion_time"] == "2025-02-01T09:30:00.000000Z"
    with pytest.raises(ValueError, match="timezone-aware"):
        fixed_clock(datetime(2025, 2, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        format_timestamp(datetime(2025, 2, 1))
