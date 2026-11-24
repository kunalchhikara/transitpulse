"""CLI: run the deterministic telemetry generator.

Usage:
    python -m simulator.generator
    python -m simulator.generator --max-events 500 --output events.ndjson
    python -m simulator.generator --generator-config my_gen.toml --seed 7

Writes one `TelemetryEvent` JSON line per emitted event to stdout (or
`--output`, if given). Structured JSON-lines logs go to stderr (or
`--log-file`, if given) -- never mixed into the event stream. Exits
cleanly (code 0) on SIGINT/SIGTERM, after finishing whichever event is
currently being written (never mid-line) and flushing the output stream --
see `simulator/generator/runner.py::run` and
`simulator/tests/test_graceful_shutdown.py`.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
from pathlib import Path
from typing import IO

from simulator.config.loader import load_scale_config
from simulator.generator.clock import SystemClock
from simulator.generator.config import load_generator_config
from simulator.generator.core import TelemetryGenerator
from simulator.generator.logging_setup import configure_logging
from simulator.generator.runner import run
from simulator.reference.generator import generate_reference_dataset
from simulator.schemas.telemetry import TelemetryEvent


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.generator",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scale-config",
        type=Path,
        default=None,
        help="Path to an alternate DE-01 scale-config TOML file (default: bundled default_scale.toml).",
    )
    parser.add_argument(
        "--generator-config",
        type=Path,
        default=None,
        help="Path to an alternate generator-config TOML file (default: bundled default_generator.toml).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Override the generator config's seed (determinism is keyed on this value).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after this many events (default: run until SIGINT/SIGTERM).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="File to write event JSON-lines to (default: stdout).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="File to write structured JSON-lines logs to (default: stderr).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Override the generator config's log_level (e.g. DEBUG, INFO, WARNING).",
    )
    return parser


def _make_sink(stream: IO[str]) -> object:
    def sink(event: TelemetryEvent) -> None:
        stream.write(event.model_dump_json())
        stream.write("\n")
        stream.flush()

    return sink


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    generator_config = load_generator_config(args.generator_config)
    if args.seed is not None:
        generator_config = generator_config.model_copy(update={"seed": args.seed})
    scale_config = load_scale_config(args.scale_config)

    logger = configure_logging(
        level=args.log_level or generator_config.log_level,
        file_path=args.log_file,
    )

    reference = generate_reference_dataset(scale_config)
    generator = TelemetryGenerator(
        scale_config=scale_config,
        generator_config=generator_config,
        reference=reference,
    )

    stop_requested = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        # Intentionally just flips a flag rather than raising -- this lets
        # any in-progress `time.sleep` in the pacing loop return early and
        # the loop's own top-of-iteration check end the run cleanly,
        # instead of an asynchronous KeyboardInterrupt landing mid-write.
        logger.info(
            "shutdown signal received, finishing current event and stopping",
            extra={"signal": signal.Signals(signum).name},
        )
        stop_requested.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Not a `with` block: when no --output is given this must be sys.stdout,
    # which must NOT be closed on exit -- the try/finally below closes the
    # stream only when it is a real file we opened.
    output_stream: IO[str] = (
        open(args.output, "w", encoding="utf-8")  # noqa: SIM115
        if args.output is not None
        else sys.stdout
    )
    try:
        logger.info(
            "generation started",
            extra={
                "vehicle_count": generator.vehicle_count,
                "events_per_second": generator_config.events_per_second,
                "seed": generator_config.seed,
                "max_events": args.max_events,
            },
        )
        summary = run(
            generator,
            _make_sink(output_stream),
            clock=SystemClock(),
            max_events=args.max_events,
            should_stop=stop_requested.is_set,
        )
        logger.info(
            "generation run complete",
            extra={
                "events_emitted": summary.events_emitted,
                "elapsed_seconds": round(summary.elapsed_seconds, 3),
                "stopped_early": summary.stopped_early,
            },
        )
    finally:
        if args.output is not None:
            output_stream.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
