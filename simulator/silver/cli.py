"""CLI: build a Silver snapshot of the DE-05 Bronze telemetry stream.

Usage:
    python -m simulator.silver --run-id s1
    python -m simulator.silver --run-id s1 --bronze-dir simulator/_out/bronze --output-dir simulator/_out/silver
    python -m simulator.silver --run-id s2 --bronze-run-id r1 --bronze-run-id r2
    python -m simulator.silver --run-id s3 --fixed-processing-time 2025-01-01T00:00:00Z

Reads only committed Bronze runs (via their manifests), validates every
record against the PROJECT.md section 12 rules, quarantines failures with
their raw payload, excludes duplicate event_ids, flags late/out-of-order
events against an event-time watermark, enriches valid events from DE-01
reference data, and commits clean/quarantine/duplicates tables plus a run
manifest whose reconciliation identity is asserted before commit. One JSON
summary line is printed to stdout; structured JSON-lines logs go to stderr
(or `--log-file`).

Exit codes: 0 committed (or already committed with identical input), 1 the
run was refused/failed (no committed Bronze input, unknown Bronze run id,
run-id conflict, reconciliation failure) - nothing is committed on failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from simulator.bronze.ingest import fixed_clock, system_clock
from simulator.config.loader import load_scale_config, load_validation_thresholds
from simulator.generator.logging_setup import JsonLinesFormatter
from simulator.silver.config import load_silver_config
from simulator.silver.errors import SilverError
from simulator.silver.pipeline import LOGGER_NAME, run_silver


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
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--fixed-processing-time must include a UTC offset (e.g. ...Z)")
    return parsed


def default_run_id() -> str:
    """Wall-clock + random suffix; pass `--run-id` for reproducible runs."""
    return f"silver-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.silver", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run-id", default=None, help="Silver run identifier (default: wall clock + random).")
    parser.add_argument("--bronze-dir", type=Path, default=None, help="Bronze store root (default: config bronze_dir).")
    parser.add_argument("--output-dir", type=Path, default=None, help="Silver store root (default: config output_dir).")
    parser.add_argument(
        "--bronze-run-id",
        action="append",
        default=None,
        help="Consume only this committed Bronze run (repeatable; default: every committed run of the stream table).",
    )
    parser.add_argument(
        "--fixed-processing-time",
        type=_parse_aware_timestamp,
        default=None,
        help="Inject a fixed clock (ISO-8601 with offset) for reproducible output; default is the wall clock.",
    )
    parser.add_argument("--silver-config", type=Path, default=None, help="Alternate Silver config TOML.")
    parser.add_argument("--validation-config", type=Path, default=None, help="Alternate DE-01 validation TOML.")
    parser.add_argument("--scale-config", type=Path, default=None, help="Alternate DE-01 scale TOML (reference data).")
    parser.add_argument("--log-file", type=Path, default=None, help="Write JSON-lines logs here (default: stderr).")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_file)
    run_id = args.run_id if args.run_id is not None else default_run_id()
    clock = fixed_clock(args.fixed_processing_time) if args.fixed_processing_time is not None else system_clock
    try:
        result = run_silver(
            run_id=run_id,
            bronze_dir=args.bronze_dir,
            output_dir=args.output_dir,
            bronze_run_ids=args.bronze_run_id,
            config=load_silver_config(args.silver_config),
            thresholds=load_validation_thresholds(args.validation_config),
            scale_config=load_scale_config(args.scale_config),
            clock=clock,
            logger=logger,
        )
    except (SilverError, ValueError) as exc:
        logger.error("silver run refused/failed", extra={"silver_run_id": run_id, "error_type": type(exc).__name__})
        print(json.dumps({"status": "FAILED", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)}))
        return 1
    counters = result.manifest["counters"]
    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.run_id,
                "manifest": str(result.manifest_path),
                "bronze_runs_consumed": [run["run_id"] for run in result.manifest["bronze_runs_consumed"]],
                **{
                    key: counters[key]
                    for key in (
                        "extracted", "valid_loaded", "quarantined", "duplicates_excluded", "late", "out_of_order",
                        "parse_errors", "schema_failures", "rule_failures", "quality_percentage",
                        "quarantined_by_rule",
                    )
                },
                "reconciliation_holds": result.manifest["reconciliation"]["holds"],
            }
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
