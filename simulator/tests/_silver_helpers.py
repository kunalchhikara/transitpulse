"""Shared helpers for the DE-06 Silver test suite (not collected by pytest:
no `test_` prefix).

The incident oracle (`derive_expectations`) deliberately does NOT use any
Silver code. It derives the expected Silver outcome from:

1. the DE-03 structured incident log (which tick was made malformed and with
   which mode; which tick was made late and by how many seconds; which
   event_ids were replayed),
2. a clean, separately constructed DE-02 generator with the same config
   (tick -> event_id and baseline event_time - DE-02's determinism
   contract), and
3. the delivered event_id sequence (identity and arrival order only - no
   field value is read to classify a record).
"""

from __future__ import annotations

import io
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from simulator.bronze import fixed_clock, ingest_stream
from simulator.config import load_scale_config, load_validation_thresholds
from simulator.generator.clock import FakeClock
from simulator.generator.runner import run
from simulator.incidents.config import IncidentsConfig
from simulator.incidents.injector import IncidentInjector
from simulator.incidents.logging_setup import configure_incident_logging
from simulator.reference.generator import generate_reference_dataset
from simulator.silver import load_silver_config, run_silver
from simulator.silver.rules import ArrivalTimeSkew, TelemetryValidator
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    REPO_ROOT,
    read_jsonl,
    write_lines,
)
from simulator.tests._generator_helpers import build_generator

PROCESSING_TIME = datetime(2025, 2, 1, 10, 0, 0, tzinfo=UTC)
PROCESSING_TIME_STR = "2025-02-01T10:00:00.000000Z"
PROCESSING_CLOCK = fixed_clock(PROCESSING_TIME)
SILVER_CONFIG = load_silver_config()
THRESHOLDS = load_validation_thresholds()
DELETE = object()
#: The DE-08 service area and (opt-in, default off) arrival-time-skew rule
#: exactly as `run_silver` passes them to `TelemetryValidator` by default.
PIPELINE_RULE_SETTINGS = {
    "service_area": load_scale_config().sydney_bounding_box,
    "arrival_time_skew": ArrivalTimeSkew.from_config(SILVER_CONFIG.arrival_time_skew),
}
#: The opt-in clock-relative rule switched on (`mode = "quarantine"`).
ARRIVAL_TIME_SKEW_ON = ArrivalTimeSkew.from_config(
    type(SILVER_CONFIG.arrival_time_skew).model_validate(
        {**SILVER_CONFIG.arrival_time_skew.model_dump(mode="json"), "mode": "quarantine"})
)

#: Expected primary Silver rule for each DE-03 malformed mode.
MODE_RULES = {
    "null_vehicle_id": "required_fields",
    "invalid_coordinates": "valid_coordinates",
    "negative_speed": "non_negative_speed",
    "malformed_timestamp": "parsable_timestamps",
    "unknown_route": "known_route",
    "missing_field": "required_fields",
}


# -- payloads ------------------------------------------------------------------


def clean_payloads(count: int) -> list[dict[str, Any]]:
    generator = build_generator()
    return [generator.produce_event(tick).model_dump(mode="json") for tick in range(count)]


def valid_payload(**overrides: Any) -> dict[str, Any]:
    """DE-02 event 0 as a JSON dict, with overrides (`DELETE` removes a key)."""
    payload = clean_payloads(1)[0]
    for key, value in overrides.items():
        if value is DELETE:
            payload.pop(key, None)
        else:
            payload[key] = value
    return payload


def validator(*, service_area=None, arrival_time_skew=None, **threshold_overrides: Any) -> TelemetryValidator:
    """Without `service_area` / `arrival_time_skew` (the default) the DE-08
    `service_area` and `arrival_time_skew` rules are not evaluated - pass
    `**PIPELINE_RULE_SETTINGS` to validate exactly as `run_silver` does."""
    thresholds = THRESHOLDS.model_copy(update=threshold_overrides) if threshold_overrides else THRESHOLDS
    reference = generate_reference_dataset(load_scale_config())
    return TelemetryValidator(thresholds, [r.route_id for r in reference.routes], service_area=service_area,
                              arrival_time_skew=arrival_time_skew)


def validate(payload: dict[str, Any], *, ingestion_time: datetime = PROCESSING_TIME, service_area=None,
             arrival_time_skew=None, **threshold_overrides: Any):
    return validator(service_area=service_area, arrival_time_skew=arrival_time_skew,
                     **threshold_overrides).validate(payload, json.dumps(payload), ingestion_time=ingestion_time)


# -- Bronze -> Silver ----------------------------------------------------------


def bronze_from_lines(
    bronze_dir: Path, lines: list[str], *, run_id: str = "b1", name: str | None = None,
    clock=FIXED_CLOCK, allow_replay: bool = False,
) -> Path:
    src_dir = bronze_dir.parent / "inputs"
    src_dir.mkdir(parents=True, exist_ok=True)
    src = write_lines(src_dir / (name or f"{run_id}.ndjson"), lines)
    ingest_stream([src], run_id=run_id, output_dir=bronze_dir, clock=clock, allow_replay=allow_replay)
    return bronze_dir


def bronze_from_bytes(bronze_dir: Path, data: bytes, *, run_id: str = "b1", clock=FIXED_CLOCK) -> Path:
    src_dir = bronze_dir.parent / "inputs"
    src_dir.mkdir(parents=True, exist_ok=True)
    src = src_dir / f"{run_id}.ndjson"
    src.write_bytes(data)
    ingest_stream([src], run_id=run_id, output_dir=bronze_dir, clock=clock)
    return bronze_dir


def silver(bronze_dir: Path, output_dir: Path | None = None, *, run_id: str = "s1", **kwargs: Any):
    out = output_dir if output_dir is not None else bronze_dir.parent / "silver"
    kwargs.setdefault("clock", PROCESSING_CLOCK)
    return run_silver(run_id=run_id, bronze_dir=bronze_dir, output_dir=out, **kwargs)


def table(result, role: str) -> list[dict[str, Any]]:
    root = result.manifest_path.parent.parent
    return read_jsonl(root / result.manifest["tables"][role]["path"])


def lines_to_silver(tmp_path: Path, lines: list[str], **kwargs: Any):
    bronze = bronze_from_lines(tmp_path / "bronze", lines)
    return silver(bronze, **kwargs)


# -- DE-03 incident stream + log -----------------------------------------------


@dataclass
class IncidentRun:
    lines: list[str]
    log: list[dict[str, Any]]
    injector: IncidentInjector
    max_events: int

    @property
    def total_ticks(self) -> int:
        return self.max_events + (
            self.injector.incidents_config.out_of_order.hold_ticks
            if self.injector.incidents_config.out_of_order.enabled
            else 0
        )


def run_incident_stream(incidents_config: IncidentsConfig, max_events: int, *, generator_config=None) -> IncidentRun:
    """Run DE-03 exactly like its CLI (DE-02 runner with the rate hook and
    the injector's duplicate-delivering sink, then flush the reorder
    buffer), on a fake clock, capturing the structured incident log.
    `generator_config` (default: DE-02's) e.g. moves `simulated_start_time`."""
    stream = io.StringIO()
    configure_incident_logging(level="INFO", stream=stream)
    try:
        generator = build_generator(generator_config=generator_config) if generator_config is not None \
            else build_generator()
        injector = IncidentInjector(generator, incidents_config)
        payloads: list[dict[str, Any]] = []
        run(injector, injector.delivery_sink(payloads.append), clock=FakeClock(), max_events=max_events,
            rate_multiplier=injector.rate_multiplier)
        payloads.extend(injector.flush_pending())
    finally:
        logger = logging.getLogger("simulator.incidents")
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
    lines = [json.dumps(p, default=str) for p in payloads]
    log = [json.loads(line) for line in stream.getvalue().splitlines()]
    return IncidentRun(lines=lines, log=log, injector=injector, max_events=max_events)


def parse_incident_log(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def fired(log: list[dict[str, Any]], scenario: str) -> list[dict[str, Any]]:
    return [
        r["context"] for r in log
        if r.get("context", {}).get("scenario") == scenario and r["message"].endswith("incident fired")
    ]


@dataclass
class Expectations:
    extracted: int = 0
    valid_loaded: int = 0
    quarantined: int = 0
    duplicates_excluded: int = 0
    in_order: int = 0
    out_of_order: int = 0
    late: int = 0
    quarantined_by_rule: Counter = field(default_factory=Counter)
    late_ids: set = field(default_factory=set)
    valid_ids: set = field(default_factory=set)
    tick_of: dict = field(default_factory=dict)
    #: event_id -> expected `_silver_event_time_status` of its clean record.
    status_of: dict = field(default_factory=dict)


def derive_expectations(
    log: list[dict[str, Any]], delivered_ids: list[str], total_ticks: int,
    *, generator_config=None, allowed_lateness_seconds: int = SILVER_CONFIG.allowed_lateness_seconds,
) -> Expectations:
    """Independent oracle - see module docstring."""
    clean = build_generator(generator_config=generator_config) if generator_config is not None else build_generator()
    tick_of: dict[str, int] = {}
    base_time: dict[int, datetime] = {}
    for tick in range(total_ticks):
        event = clean.produce_event(tick)
        tick_of[event.event_id] = tick
        base_time[tick] = event.event_time
    malformed = {c["tick"]: c["mode"] for c in fired(log, "malformed")}
    skew = {c["tick"]: c["skew_seconds"] for c in fired(log, "late_event")}

    exp = Expectations(tick_of=tick_of)
    loaded: set[str] = set()
    max_seen: datetime | None = None
    lateness = timedelta(seconds=allowed_lateness_seconds)
    for event_id in delivered_ids:
        exp.extracted += 1
        tick = tick_of[event_id]
        if tick in malformed:
            exp.quarantined += 1
            exp.quarantined_by_rule[MODE_RULES[malformed[tick]]] += 1
            continue
        if event_id in loaded:
            exp.duplicates_excluded += 1
            continue
        loaded.add(event_id)
        exp.valid_loaded += 1
        event_time = base_time[tick] - timedelta(seconds=skew.get(tick, 0))
        if max_seen is None or event_time >= max_seen:
            exp.in_order += 1
            exp.status_of[event_id] = "in_order"
        elif event_time < max_seen - lateness:
            exp.late += 1
            exp.late_ids.add(event_id)
            exp.status_of[event_id] = "late"
        else:
            exp.out_of_order += 1
            exp.status_of[event_id] = "out_of_order"
        max_seen = event_time if max_seen is None or event_time > max_seen else max_seen
    exp.valid_ids = loaded
    return exp


def delivered_event_ids(lines: list[str]) -> list[str]:
    """Identity + arrival order only (DE-03 never corrupts event_id)."""
    return [json.loads(line)["event_id"] for line in lines]


__all__ = [
    "ARRIVAL_TIME_SKEW_ON", "DELETE", "MODE_RULES", "PIPELINE_RULE_SETTINGS", "PROCESSING_CLOCK", "PROCESSING_TIME", "PROCESSING_TIME_STR", "REPO_ROOT",
    "SILVER_CONFIG", "THRESHOLDS", "Expectations", "IncidentRun", "bronze_from_bytes", "bronze_from_lines",
    "clean_payloads", "delivered_event_ids", "derive_expectations", "fired", "lines_to_silver",
    "parse_incident_log", "run_incident_stream", "silver", "table", "valid_payload", "validate", "validator",
]
