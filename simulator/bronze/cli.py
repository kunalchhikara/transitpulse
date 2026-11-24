"""CLI: ingest streaming-shaped or batch-shaped input into the local Bronze store.

Usage:
    python -m simulator.bronze stream --input events.ndjson --run-id r1
    python -m simulator.incidents --enable-malformed --max-events 200 \\
        | python -m simulator.bronze stream --input - --run-id r2
    python -m simulator.bronze batch --input-dir simulator/_out/batch --run-id b1
    python -m simulator.bronze batch --input-dir DIR --output-dir OUT --fixed-ingestion-time 2025-01-01T00:00:00Z

Every input line becomes exactly one Bronze record (original object +
`ingestion_time`/`source`/`run_id` + `_bronze_*` lineage) or a marked
parse-error record holding the raw line. A per-run audit manifest is written
to `<output-dir>/_runs/<run_id>.json`. One JSON summary line is printed to
stdout; structured JSON-lines logs go to stderr (or `--log-file`).

Exit codes: 0 committed (or already committed with identical input), 1 the
run was refused/failed (run-id conflict, duplicate input without
`--allow-replay`, parse error under `--strict`, source-manifest mismatch,
missing input) - see the error log line; nothing is committed on failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from simulator.bronze.config import ParseErrorPolicy, load_bronze_config
from simulator.bronze.ingest import (
    LOGGER_NAME,
    BronzeError,
    fixed_clock,
    ingest_batch,
    ingest_stream,
    system_clock,
)
from simulator.generator.logging_setup import JsonLinesFormatter


def _configure_logging(level: str, file_path: Path | None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    for existing in list(logger.handlers):
        logger.removeHandler(existing)
    handler: logging.Handler = (
        logging.FileHandler(file_path, encoding="utf-8") if file_path is not None else logging.StreamHandler(sys.stderr)
    )
    handler.setFormatter(JsonLinesFormatter())
    logger.addHandler(handler)
    return logger


def _parse_aware_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--fixed-ingestion-time must include a UTC offset (e.g. ...Z)")
    return parsed


def default_run_id() -> str:
    """Wall-clock + random suffix; pass `--run-id` for reproducible runs."""
    return f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", default=None, help="Run identifier (default: generated from wall clock + random).")
    parser.add_argument("--output-dir", type=Path, default=None, help="Bronze store root (default: config output_dir).")
    parser.add_argument("--bronze-config", type=Path, default=None, help="Alternate Bronze config TOML.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="parse_error_policy='fail': abort (nothing committed) on any unparseable line.",
    )
    parser.add_argument(
        "--allow-replay",
        action="store_true",
        help="Allow re-ingesting input a committed run already ingested (flagged in the manifest as replay_of).",
    )
    parser.add_argument(
        "--fixed-ingestion-time",
        type=_parse_aware_timestamp,
        default=None,
        help="Inject a fixed clock (ISO-8601 with offset) for reproducible output; default is the wall clock.",
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Write JSON-lines logs here (default: stderr).")
    parser.add_argument("--log-level", default="INFO")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.bronze", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    stream = sub.add_parser("stream", help="Ingest DE-02/DE-03 NDJSON telemetry (files or '-' for stdin).")
    stream.add_argument("--input", action="append", required=True, help="NDJSON file, or '-' for stdin. Repeatable.")
    stream.add_argument("--source", default=None, help="Override the configured stream `source` label.")
    _add_common(stream)
    batch = sub.add_parser("batch", help="Ingest a DE-04 batch dataset directory.")
    batch.add_argument("--input-dir", type=Path, default=None, help="DE-04 output directory (default: config).")
    batch.add_argument(
        "--no-verify-source-manifest",
        action="store_true",
        help="Record but do not fail on sha256/row_count mismatch against the DE-04 manifest.json.",
    )
    _add_common(batch)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_file)
    config = load_bronze_config(args.bronze_config)
    if args.strict:
        config = config.model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
    if args.mode == "batch" and args.no_verify_source_manifest:
        config = config.model_copy(update={"verify_source_manifest": False})
    run_id = args.run_id if args.run_id is not None else default_run_id()
    clock = fixed_clock(args.fixed_ingestion_time) if args.fixed_ingestion_time is not None else system_clock
    common = {
        "run_id": run_id,
        "output_dir": args.output_dir,
        "config": config,
        "clock": clock,
        "allow_replay": args.allow_replay,
        "logger": logger,
    }
    try:
        if args.mode == "stream":
            result = ingest_stream(args.input, source=args.source, **common)
        else:
            result = ingest_batch(args.input_dir, **common)
    except (BronzeError, ValueError) as exc:
        logger.error("bronze ingestion refused/failed", extra={"run_id": run_id, "error_type": type(exc).__name__})
        print(json.dumps({"status": "FAILED", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)}))
        return 1
    totals = result.manifest["totals"]
    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.run_id,
                "manifest": str(result.manifest_path),
                "records_written": totals["records_written"],
                "parse_error_records": totals["parse_error_records"],
                "replay_of": result.manifest["replay_of"],
            }
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
