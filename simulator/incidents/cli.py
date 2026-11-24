"""CLI: run the DE-02 telemetry generator with DE-03 incident injection.

Usage:
    python -m simulator.incidents
    python -m simulator.incidents --enable-malformed --enable-duplicate
    python -m simulator.incidents --max-events 500 --output events.ndjson
    python -m simulator.incidents --incidents-config my_incidents.toml

Writes one JSON-lines payload per emitted event to stdout (or `--output`).
Unlike `simulator/generator/cli.py`'s event stream (always a structurally
valid `TelemetryEvent`), a payload here may be deliberately malformed (see
`simulator/incidents/malformed.py`) -- it is a plain dict, not a
`TelemetryEvent.model_dump_json()` call, so it can hold values
`TelemetryEvent` itself could never validate. Structured JSON-lines
incident logs (one JSON object per line, recording which scenario fired/
recovered and when) go to stderr (or `--log-file`), via the
`simulator.incidents` logger -- kept separate from the event stream, same
pattern as `simulator/generator/cli.py`.

--max-events N bounds the paced main loop to N generator ticks (N
*original* events, each delivered exactly once). Delivered lines can exceed
N: when out-of-order is enabled its `hold_ticks` pre-filled originals are
flushed after the loop, and every duplicate fire adds one extra line -- a
verbatim replay of an earlier line, written right after the original that
triggered it (FU-05: a duplicate is an addition, never a replacement). So
total lines = N (+ hold_ticks if out-of-order) + duplicate fires, and the
set of event_ids is identical with duplicates on or off.

--enable-* flags are additive overrides on top of whatever `--incidents-
config` loads (default: every scenario `enabled = false`) -- each flips
exactly one scenario's `enabled` to true without needing a bespoke TOML
file, satisfying DE-03's "individually enabled via config/CLI flags"
requirement.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import IO

from simulator.config.loader import load_scale_config
from simulator.generator.clock import SystemClock
from simulator.generator.config import load_generator_config
from simulator.generator.core import TelemetryGenerator
from simulator.generator.runner import run
from simulator.incidents.config import load_incidents_config
from simulator.incidents.injector import EventPayload, IncidentInjector
from simulator.incidents.logging_setup import configure_incident_logging
from simulator.reference.generator import generate_reference_dataset

_SCENARIO_FLAGS = (
    "malformed",
    "duplicate",
    "late_event",
    "out_of_order",
    "congestion",
    "volume_spike",
    "breakdown",
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.incidents",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scale-config", type=Path, default=None)
    parser.add_argument("--generator-config", type=Path, default=None)
    parser.add_argument("--incidents-config", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=None, help="Override the generator config's seed.")
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--log-file", type=Path, default=None)
    parser.add_argument("--log-level", type=str, default="INFO")
    for name in _SCENARIO_FLAGS:
        parser.add_argument(
            f"--enable-{name.replace('_', '-')}",
            action="store_true",
            default=False,
            help=f"Force-enable the '{name}' incident scenario regardless of --incidents-config.",
        )
    return parser


def _apply_enable_flags(incidents_config, args: argparse.Namespace):
    update: dict[str, object] = {}
    for name in _SCENARIO_FLAGS:
        flag_attr = f"enable_{name}"
        if getattr(args, flag_attr, False):
            sub = getattr(incidents_config, name).model_copy(update={"enabled": True})
            update[name] = sub
    return incidents_config.model_copy(update=update) if update else incidents_config


def _make_sink(stream: IO[str]):
    def sink(payload: EventPayload) -> None:
        stream.write(json.dumps(payload, default=str))
        stream.write("\n")
        stream.flush()

    return sink


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    generator_config = load_generator_config(args.generator_config)
    if args.seed is not None:
        generator_config = generator_config.model_copy(update={"seed": args.seed})
    scale_config = load_scale_config(args.scale_config)
    incidents_config = load_incidents_config(args.incidents_config)
    incidents_config = _apply_enable_flags(incidents_config, args)

    logger = configure_incident_logging(level=args.log_level, file_path=args.log_file)

    reference = generate_reference_dataset(scale_config)
    generator = TelemetryGenerator(
        scale_config=scale_config, generator_config=generator_config, reference=reference
    )
    injector = IncidentInjector(generator, incidents_config)

    stop_requested = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info(
            "shutdown signal received, finishing current event and stopping",
            extra={"signal": signal.Signals(signum).name},
        )
        stop_requested.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    output_stream: IO[str] = (
        open(args.output, "w", encoding="utf-8")  # noqa: SIM115
        if args.output is not None
        else sys.stdout
    )
    write = _make_sink(output_stream)
    delivered = 0

    def sink(payload: EventPayload) -> None:
        nonlocal delivered
        write(payload)
        delivered += 1

    try:
        logger.info(
            "incident-aware generation started",
            extra={
                "vehicle_count": generator.vehicle_count,
                "events_per_second": generator_config.events_per_second,
                "seed": generator_config.seed,
                "incidents_seed": incidents_config.seed,
                "max_events": args.max_events,
            },
        )
        summary = run(
            injector,
            injector.delivery_sink(sink),
            clock=SystemClock(),
            max_events=args.max_events,
            should_stop=stop_requested.is_set,
            rate_multiplier=injector.rate_multiplier,
        )
        for pending in injector.flush_pending():
            sink(pending)
        logger.info(
            "incident-aware generation run complete",
            extra={
                # Generator ticks in the paced main loop (== --max-events
                # unless stopped early); see the module docstring.
                "events_emitted": summary.events_emitted,
                "payloads_delivered": delivered,
                "duplicates_injected": injector.duplicates_injected,
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
