"""CLI: generate the DE-04 batch/historical datasets to local JSON-lines files.

Usage:
    python -m simulator.batch --output-dir simulator/_out/batch
    python -m simulator.batch --output-dir out --batch-config my_batch.toml --seed 7
    python -m simulator.batch --output-dir out --scale-config my_scale.toml

Writes one `<dataset>.jsonl` per dataset in `DATASET_CATALOGUE` plus
`manifest.json`. Same config + seed -> byte-identical files across separate
runs (see `simulator/tests/test_batch_determinism.py`). Structured JSON-lines
logs go to stderr (or `--log-file`). Default output lives under
`simulator/_out/`, which `simulator/.gitignore` excludes from Git.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from simulator.batch.config import load_batch_config
from simulator.batch.generator import generate_batch_datasets
from simulator.batch.writer import write_batch_datasets
from simulator.config.loader import load_scale_config
from simulator.generator.logging_setup import JsonLinesFormatter

LOGGER_NAME = "simulator.batch"
DEFAULT_OUTPUT_DIR = Path("simulator/_out/batch")


def _configure_logging(level: str, file_path: Path | None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    for existing in list(logger.handlers):
        logger.removeHandler(existing)
    handler: logging.Handler = (
        logging.FileHandler(file_path, encoding="utf-8")
        if file_path is not None
        else logging.StreamHandler()
    )
    handler.setFormatter(JsonLinesFormatter())
    logger.addHandler(handler)
    return logger


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.batch",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write <dataset>.jsonl + manifest.json into (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--batch-config",
        type=Path,
        default=None,
        help="Alternate DE-04 batch-config TOML (default: simulator/batch/default_batch.toml).",
    )
    parser.add_argument(
        "--scale-config",
        type=Path,
        default=None,
        help="Alternate DE-01 scale-config TOML (default: bundled default_scale.toml).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Override the batch config's seed (does not change the DE-01 reference seed).",
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Write JSON-lines logs here (default: stderr).")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level (default: INFO).")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_file)

    batch_config = load_batch_config(args.batch_config)
    if args.seed is not None:
        batch_config = batch_config.with_seed(args.seed)
    scale_config = load_scale_config(args.scale_config)

    logger.info(
        "batch generation started",
        extra={
            "batch_seed": batch_config.batch.seed,
            "reference_seed": scale_config.seed,
            "start_date": batch_config.batch.start_date.isoformat(),
            "service_days": batch_config.batch.service_days,
            "output_dir": str(args.output_dir),
        },
    )
    datasets = generate_batch_datasets(batch_config, scale_config)
    if datasets.vehicle_overlap_count:
        logger.warning(
            "vehicle pool too small for timetable; some trips reuse a busy vehicle",
            extra={"vehicle_overlap_count": datasets.vehicle_overlap_count},
        )
    manifest = write_batch_datasets(
        datasets, args.output_dir, batch_config=batch_config, scale_config=scale_config
    )
    for name, entry in manifest["datasets"].items():
        logger.info(
            "dataset written",
            extra={
                "dataset": name,
                "file": str(args.output_dir / entry["file"]),
                "row_count": entry["row_count"],
                "sha256": entry["sha256"],
            },
        )
    logger.info(
        "batch generation complete",
        extra={"total_rows": sum(e["row_count"] for e in manifest["datasets"].values())},
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
