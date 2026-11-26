"""CLI: batch Silver (DE-12) - apply committed Bronze batch runs to the
current-state tables.

Usage:
    python -m simulator.silver_batch --run-id sb-001
    python -m simulator.silver_batch --run-id sb-002 --bronze-dir simulator/_out/bronze \\
        --output-dir simulator/_out/silver_batch --incremental-state-dir simulator/_out/incremental
    python -m simulator.silver_batch --run-id sb-003 --bronze-run-id inc-schedules-initial-20250106T000000Z
    python -m simulator.silver_batch --run-id sb-004 --fixed-processing-time 2030-01-01T00:00:00Z

Applies every committed Bronze batch run not yet in the checkpoint (or only
`--bronze-run-id` runs, still in Bronze commit order), validates every row
strictly against its DE-04/DE-01 model, quarantines failures with their raw
payload, merges valid rows by primary key (inserted / updated / unchanged /
stale / conflict; reference snapshots also deleted), reconciles every
dataset and commits a run manifest. One JSON summary line goes to stdout;
JSON-lines logs (every line carries `silver_batch_run_id`) go to stderr or
`--log-file` (appended to - the run id tells runs apart).

Exit codes: 0 committed (or ALREADY_COMMITTED with identical input);
1 the run failed (Bronze contract / reconciliation / snapshot-delete guard) -
nothing committed, a FAILED manifest records why; 2 refused before anything
was written (invalid or conflicting run id, unknown Bronze run id, invalid
config, unusable log file, inconsistent Silver-batch store).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from simulator.bronze.ingest import fixed_clock, system_clock
from simulator.bronze.store import InvalidRunIdError
from simulator.generator.logging_setup import JsonLinesFormatter
from simulator.silver_batch.config import load_silver_batch_config
from simulator.silver_batch.errors import SilverBatchError, SilverBatchRefusedError
from simulator.silver_batch.pipeline import LOGGER_NAME, run_silver_batch

EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 2


def _parse_aware_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--fixed-processing-time must include a UTC offset (e.g. ...Z)")
    return parsed


def default_run_id() -> str:
    """Wall clock + random suffix; pass `--run-id` for reproducible runs."""
    return f"sb-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.silver_batch", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--run-id", default=None, help="Batch-Silver run id (default: wall clock + random).")
    parser.add_argument("--bronze-dir", type=Path, default=None, help="Bronze store root (config bronze_dir).")
    parser.add_argument("--output-dir", type=Path, default=None, help="Batch-Silver store root (config output_dir).")
    parser.add_argument("--incremental-state-dir", type=Path, default=None,
                        help="DE-11 state dir holding the extract manifests (config incremental_state_dir).")
    parser.add_argument("--no-incremental-listing", action="store_true",
                        help="Do not read DE-11 extract manifests (strict validation still applies).")
    parser.add_argument("--bronze-run-id", action="append", default=None,
                        help="Apply only this committed Bronze run (repeatable; default: every pending run).")
    parser.add_argument("--fixed-processing-time", type=_parse_aware_timestamp, default=None,
                        help="Fixed clock (ISO-8601 with offset) for reproducible output; default wall clock.")
    parser.add_argument("--config", type=Path, default=None, help="Alternate batch-Silver config TOML.")
    parser.add_argument("--log-file", type=Path, default=None, help="Append JSON-lines logs here (default stderr).")
    parser.add_argument("--log-level", default="INFO")
    return parser


def _configure_logging(level: str, file_path: Path | None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    for existing in list(logger.handlers):
        logger.removeHandler(existing)
        existing.close()
    handler: logging.Handler = (
        logging.FileHandler(file_path, encoding="utf-8") if file_path is not None else logging.StreamHandler(sys.stderr)
    )
    handler.setFormatter(JsonLinesFormatter())
    logger.addHandler(handler)
    return logger


def _refuse(run_id: str | None, exc: BaseException) -> int:
    print(json.dumps({"status": "REFUSED", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)}))
    return EXIT_REFUSED


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    run_id = args.run_id if args.run_id is not None else default_run_id()
    try:
        logger = _configure_logging(args.log_level, args.log_file)
    except (OSError, ValueError) as exc:  # FU-12: structured refusal, not a traceback
        return _refuse(run_id, exc)
    try:
        config = load_silver_batch_config(args.config)
        updates: dict[str, object] = {}
        if args.incremental_state_dir is not None:
            updates["incremental_state_dir"] = args.incremental_state_dir
        if args.no_incremental_listing:
            updates["incremental_state_dir"] = None
        if updates:
            config = config.model_validate({**config.model_dump(), **updates})
    except (OSError, ValidationError, KeyError, ValueError) as exc:
        logger.error("config refused", extra={"silver_batch_run_id": run_id, "error_type": type(exc).__name__})
        return _refuse(run_id, exc)
    clock = fixed_clock(args.fixed_processing_time) if args.fixed_processing_time is not None else system_clock
    try:
        result = run_silver_batch(
            run_id=run_id,
            bronze_dir=args.bronze_dir,
            output_dir=args.output_dir,
            bronze_run_ids=args.bronze_run_id,
            config=config,
            clock=clock,
            logger=logger,
        )
    except (SilverBatchRefusedError, InvalidRunIdError) as exc:
        logger.error("batch-silver run refused", extra={"silver_batch_run_id": run_id,
                                                         "error_type": type(exc).__name__})
        return _refuse(run_id, exc)
    except SilverBatchError as exc:
        print(json.dumps({"status": "FAILED", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)}))
        return EXIT_FAILED
    manifest = result.manifest
    print(json.dumps({
        "status": result.status,
        "run_id": result.run_id,
        "commit_sequence": manifest["commit_sequence"],
        "base_run_id": manifest["base_run_id"],
        "manifest": f"_runs/{result.manifest_path.name}",
        "bronze_runs_applied": [run["run_id"] for run in manifest["bronze_runs_applied"]],
        "bronze_runs_skipped_already_applied": manifest["bronze_runs_skipped_already_applied"],
        "datasets": {
            name: {key: info["counters"][key] for key in (
                "extracted", "inserted", "updated", "unchanged", "stale", "quarantined", "deleted")}
            for name, info in manifest["datasets"].items()
        },
        "reconciliation_holds": manifest["reconciliation"]["holds"],
    }, sort_keys=True))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
