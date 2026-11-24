"""CLI: build the DE-07 Gold route-performance aggregate from one committed
Silver run.

Usage:
    python -m simulator.gold --run-id g1
    python -m simulator.gold --run-id g1 --silver-dir simulator/_out/silver --output-dir simulator/_out/gold
    python -m simulator.gold --run-id g2 --silver-run-id s1
    python -m simulator.gold --run-id g3 --fixed-processing-time 2025-01-01T00:00:00Z

Reads only committed Silver runs (via their manifests; default: the latest
committed one, since each Silver run is a full snapshot), aggregates the
clean records per route per period (late rows included; quarantined and
duplicate records excluded), attributes quarantined records to a route and
period where their route_id/event_time are usable, and commits the
aggregate plus a run manifest whose reconciliation against the Silver
counters is asserted before commit. One JSON summary line is printed to
stdout; structured JSON-lines logs go to stderr (or `--log-file`).

Exit codes: 0 committed (or already committed with identical input), 1 the
run was refused/failed - nothing is committed on failure.
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
from simulator.config.loader import load_scale_config
from simulator.generator.logging_setup import JsonLinesFormatter
from simulator.gold.config import load_gold_config
from simulator.gold.errors import GoldError
from simulator.gold.pipeline import LOGGER_NAME, run_gold


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
    return f"gold-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.gold", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run-id", default=None, help="Gold run identifier (default: wall clock + random).")
    parser.add_argument("--silver-dir", type=Path, default=None, help="Silver store root (default: config silver_dir).")
    parser.add_argument("--output-dir", type=Path, default=None, help="Gold store root (default: config output_dir).")
    parser.add_argument(
        "--silver-run-id", default=None,
        help="Aggregate this committed Silver run (default: the latest committed run).",
    )
    parser.add_argument(
        "--fixed-processing-time",
        type=_parse_aware_timestamp,
        default=None,
        help="Inject a fixed clock (ISO-8601 with offset) for reproducible output; default is the wall clock.",
    )
    parser.add_argument("--gold-config", type=Path, default=None, help="Alternate Gold config TOML.")
    parser.add_argument(
        "--scale-config", type=Path, default=None,
        help="Alternate DE-01 scale TOML (reference data; must be the one Silver used).",
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Write JSON-lines logs here (default: stderr).")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_file)
    run_id = args.run_id if args.run_id is not None else default_run_id()
    clock = fixed_clock(args.fixed_processing_time) if args.fixed_processing_time is not None else system_clock
    try:
        result = run_gold(
            run_id=run_id,
            silver_dir=args.silver_dir,
            output_dir=args.output_dir,
            silver_run_id=args.silver_run_id,
            config=load_gold_config(args.gold_config),
            scale_config=load_scale_config(args.scale_config),
            clock=clock,
            logger=logger,
        )
    except (GoldError, ValueError) as exc:
        logger.error("gold run refused/failed", extra={"gold_run_id": run_id, "error_type": type(exc).__name__})
        print(json.dumps({"status": "FAILED", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)}))
        return 1
    counters = result.manifest["counters"]
    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.run_id,
                "manifest": str(result.manifest_path),
                "silver_runs_consumed": [run["run_id"] for run in result.manifest["silver_runs_consumed"]],
                "silver_selection": result.manifest["silver_selection"],
                **{
                    key: counters[key]
                    for key in (
                        "rows", "routes", "periods", "event_count", "late_event_count", "quarantined_attributed",
                        "quarantined_unattributed", "unattributed_by_reason", "duplicates_excluded",
                        "overall_valid_percentage", "replay_sensitive",
                    )
                },
                "reconciliation_holds": result.manifest["reconciliation"]["holds"],
            }
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
