"""Shared helpers for the DE-05 Bronze test suite (not collected by pytest:
no `test_` prefix)."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simulator.bronze.ingest import fixed_clock
from simulator.bronze.records import (
    INGESTION_TIME,
    LINE_NUMBER,
    RUN_ID,
    SOURCE,
    SOURCE_FILE,
    STORED_RECORD_METADATA_FIELDS,
    strip_metadata,
)
from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
)
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_BATCH_DIR = REPO_ROOT / "simulator" / "batch" / "sample" / "output"
FAST_GENERATOR_CONFIG = Path(__file__).resolve().parent / "fixtures" / "fast_generator.toml"

FIXED_TIME = datetime(2025, 2, 1, 9, 30, 0, tzinfo=UTC)
FIXED_TIME_STR = "2025-02-01T09:30:00.000000Z"
FIXED_CLOCK = fixed_clock(FIXED_TIME)


def clean_stream_lines(count: int) -> list[str]:
    """DE-02 generator output exactly as its CLI writes it (`model_dump_json`)."""
    generator = build_generator()
    return [generator.produce_event(tick).model_dump_json() for tick in range(count)]


def incident_stream_payloads(count: int = 120) -> list[dict[str, Any]]:
    """A DE-03 stream with every payload-affecting scenario enabled: all six
    malformed modes, duplicates, late and out-of-order delivery, congestion
    and breakdown. Deterministic (fixed seeds)."""
    probe = build_generator()
    first = probe.produce_event(0)
    injector = build_injector(
        malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.25),
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.15),
        late_event=LateEventConfig(enabled=True, probability_per_tick=0.15),
        out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.5, hold_ticks=3),
        congestion=CongestionConfig(
            enabled=True, route_id=first.route_id, window=RecoveryWindow(start_tick=0, duration_ticks=count)
        ),
        breakdown=BreakdownConfig(
            enabled=True, vehicle_ids=[first.vehicle_id], window=RecoveryWindow(start_tick=0, duration_ticks=count)
        ),
    )
    payloads = [injector.produce_event(tick) for tick in range(count)]
    return payloads + injector.flush_pending()


def incident_stream_lines(count: int = 120) -> list[str]:
    """Serialized exactly like `simulator/incidents/cli.py`'s sink."""
    return [json.dumps(payload, default=str) for payload in incident_stream_payloads(count)]


def write_lines(path: Path, lines: list[str], *, trailing_newline: bool = True) -> Path:
    text = "\n".join(lines) + ("\n" if trailing_newline and lines else "")
    path.write_bytes(text.encode("utf-8"))
    return path


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def canonical(value: Any) -> str:
    """Type- and order-strict representation (1 != 1.0 != true; key order kept)."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def assert_round_trip(
    input_lines: list[str],
    bronze_records: list[dict[str, Any]],
    bronze_lines: list[str],
    *,
    run_id: str,
    source: str,
    source_file: str,
    ingestion_time: str = FIXED_TIME_STR,
) -> None:
    """Every input line is a JSON object stored as exactly one Bronze record:
    original fields unmodified (value, type, key order AND bytes) plus exactly
    the stored-record metadata."""
    assert len(bronze_records) == len(input_lines) == len(bronze_lines)
    for number, (line, record, out_line) in enumerate(zip(input_lines, bronze_records, bronze_lines), start=1):
        original = json.loads(line)
        assert canonical(strip_metadata(record)) == canonical(original), f"line {number} changed"
        assert list(record)[: len(original)] == list(original), f"line {number} key order changed"
        assert set(record) - set(original) == set(STORED_RECORD_METADATA_FIELDS)
        assert record[INGESTION_TIME] == ingestion_time
        assert record[SOURCE] == source
        assert record[RUN_ID] == run_id
        assert record[SOURCE_FILE] == source_file
        assert record[LINE_NUMBER] == number
        # Byte-level: the Bronze line is the original object's own text with
        # metadata spliced in before the closing brace.
        assert out_line.startswith(line.strip()[:-1]), f"line {number} bytes changed"


def run_cli(*args: str, stdin: bytes | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "simulator.bronze", *args],
        cwd=REPO_ROOT,
        input=stdin,
        capture_output=True,
        check=False,
    )
