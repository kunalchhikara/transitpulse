"""CLI: incremental batch extraction with watermark control into Bronze (DE-11).

Usage:
    python -m simulator.reference --output-dir simulator/_out/reference
    python -m simulator.batch --output-dir simulator/_out/batch
    python -m simulator.incremental --as-of 2025-01-06T12:00:00+10:00
    python -m simulator.incremental --as-of 2025-01-07T00:00:00Z --fixed-clock 2025-02-01T00:00:00Z

`--as-of` (required, ISO-8601 with a UTC offset) is the extraction time: each
incremental dataset loads `watermark < updated_at <= as_of`, where the
watermark is the maximum `updated_at` already committed for that dataset
(everything up to `as_of` until a row is committed); reference entities are
reloaded in full and skipped when unchanged. One JSON summary line goes to
stdout; structured JSON-lines logs (every line carries `pipeline_run_id`) go
to stderr or `--log-file` (appended to - the run id tells runs apart).

Exit codes: 0 every dataset succeeded (incl. SKIPPED_UNCHANGED/UP_TO_DATE);
1 PARTIAL or FAILED (see the audit records; failed watermarks unchanged);
2 refused before anything was written (bad `--as-of` or one earlier than a
dataset's last committed as_of, invalid config, reused `--pipeline-run-id`,
a control record of another schema version);
3 SUCCEEDED_WITH_WARNINGS: every dataset succeeded but late rows were
skipped (or source rows vanished) under `--late-arrival-policy record` -
see `late_rows_skipped` / `warnings`.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pydantic import ValidationError

from simulator.generator.logging_setup import JsonLinesFormatter
from simulator.incremental.clock import ManualClock, SystemClock, format_ts, parse_aware
from simulator.incremental.config import (
    BadSourcePolicy,
    LateArrivalPolicy,
    load_incremental_config,
)
from simulator.incremental.loader import (
    BRONZE_LOGGER_NAME,
    LOGGER_NAME,
    PIPELINE_FAILED,
    PIPELINE_PARTIAL,
    PIPELINE_SUCCEEDED,
    PIPELINE_SUCCEEDED_WITH_WARNINGS,
    AsOfError,
    run_incremental_load,
)
from simulator.incremental.state import StateError


def _aware(flag: str):
    def parse(value: str):
        try:
            return parse_aware(value, flag)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(str(exc)) from exc

    return parse


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.incremental",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--as-of", required=True, type=_aware("--as-of"), help="Extraction time, with UTC offset.")
    parser.add_argument("--config", type=Path, default=None, help="Alternate loader config TOML.")
    parser.add_argument("--state-dir", type=Path, default=None, help="Control/audit/extract root (config state_dir).")
    parser.add_argument("--batch-source-dir", type=Path, default=None, help="DE-04 source directory.")
    parser.add_argument("--reference-source-dir", type=Path, default=None, help="DE-01 reference JSON directory.")
    parser.add_argument("--bronze-output-dir", type=Path, default=None, help="DE-05 Bronze store root.")
    parser.add_argument("--bronze-config", type=Path, default=None, help="Alternate DE-05 Bronze config TOML.")
    parser.add_argument(
        "--bad-source-policy", choices=[p.value for p in BadSourcePolicy], default=None, help="strict | preserve."
    )
    parser.add_argument(
        "--late-arrival-policy", choices=[p.value for p in LateArrivalPolicy], default=None, help="fail | record."
    )
    parser.add_argument(
        "--fixed-clock",
        type=_aware("--fixed-clock"),
        default=None,
        help="Virtual clock start (ISO-8601 with offset) for reproducible, byte-identical runs; retry backoff "
        "advances it without sleeping. Default: the wall clock.",
    )
    parser.add_argument(
        "--pipeline-run-id", default=None, help="Explicit pipeline run id (default: ipr-<seq>-<as_of>)."
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Append JSON-lines logs here (default: stderr).")
    parser.add_argument("--log-level", default="INFO")
    return parser


def _configure_logging(level: str, file_path: Path | None) -> logging.Logger:
    handler: logging.Handler = (
        logging.FileHandler(file_path, mode="a", encoding="utf-8")
        if file_path is not None
        else logging.StreamHandler(sys.stderr)
    )
    handler.setFormatter(JsonLinesFormatter())
    for name in (LOGGER_NAME, BRONZE_LOGGER_NAME):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False
        for existing in list(logger.handlers):
            logger.removeHandler(existing)
        logger.addHandler(handler)
    return logging.getLogger(LOGGER_NAME)


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_file)
    try:
        config = load_incremental_config(args.config)
        overrides = {
            key: value
            for key, value in {
                "state_dir": args.state_dir,
                "batch_source_dir": args.batch_source_dir,
                "reference_source_dir": args.reference_source_dir,
                "bronze_output_dir": args.bronze_output_dir,
                "bronze_config": args.bronze_config,
                "bad_source_policy": args.bad_source_policy,
                "late_arrival_policy": args.late_arrival_policy,
            }.items()
            if value is not None
        }
        if overrides:
            config = type(config).model_validate({**config.model_dump(), **overrides})
    except (OSError, ValueError, ValidationError, KeyError) as exc:
        logger.error("invalid loader config", extra={"pipeline_run_id": None, "error": str(exc)})
        print(json.dumps({"status": "REFUSED", "error_type": type(exc).__name__, "error": str(exc)}))
        return 2
    clock = ManualClock(args.fixed_clock) if args.fixed_clock is not None else SystemClock()
    try:
        result = run_incremental_load(
            args.as_of, config=config, clock=clock, logger=logger, pipeline_run_id=args.pipeline_run_id
        )
    except (AsOfError, StateError) as exc:
        print(
            json.dumps(
                {"status": "REFUSED", "as_of": format_ts(args.as_of), "error_type": type(exc).__name__, "error": str(exc)}
            )
        )
        return 2
    print(
        json.dumps(
            {
                "status": result.status,
                "pipeline_run_id": result.pipeline_run_id,
                "as_of": result.as_of,
                "summary": result.summary_path,
                "late_rows_skipped": result.late_rows_skipped,
                "warnings": result.warnings,
                "datasets": {
                    r.dataset: {
                        "status": r.status,
                        "attempts": r.attempts,
                        "watermark_after": r.watermark_after,
                        "last_as_of_after": r.last_as_of_after,
                        "extracted_count": r.extracted_count,
                        "late_rows_skipped": r.late_rows_skipped,
                        "error_type": r.error["type"] if r.error else None,
                    }
                    for r in result.datasets
                },
            },
            sort_keys=True,
        )
    )
    return EXIT_CODES.get(result.status, 1)


#: Process exit code per pipeline status (2 = refused, see `main`).
EXIT_CODES = {PIPELINE_SUCCEEDED: 0, PIPELINE_SUCCEEDED_WITH_WARNINGS: 3, PIPELINE_PARTIAL: 1, PIPELINE_FAILED: 1}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
