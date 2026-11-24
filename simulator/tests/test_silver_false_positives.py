"""DE-08 criterion 4: with the default configuration, the new Silver rules
(`service_area`, `delay_range`, `timestamp_bounds`, `watermark_guard`)
produce ZERO new quarantines on valid simulator output:

* DE-02 baseline output (no incidents);
* DE-03 congestion and breakdown output (degraded but valid values);
* DE-04 default `historical_telemetry` (default batch + scale config).

Each sweep goes through the real DE-05 Bronze and DE-06 Silver code, so the
rules are applied exactly as `run_silver` applies them (DE-01 service area,
Bronze ingestion time as the watermark-guard reference)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from simulator.batch.config import load_batch_config
from simulator.batch.generator import generate_batch_datasets
from simulator.bronze import fixed_clock
from simulator.config import load_scale_config
from simulator.incidents.config import BreakdownConfig, CongestionConfig, RecoveryWindow
from simulator.reference.generator import generate_reference_dataset
from simulator.schemas.telemetry import TelemetryEvent
from simulator.tests._incident_helpers import baseline_incidents_config
from simulator.tests._silver_helpers import (
    bronze_from_lines,
    run_incident_stream,
    silver,
)

NEW_RULES = ("service_area", "delay_range", "timestamp_bounds", "watermark_guard")
EVENTS = 3000


def _assert_no_quarantine(result, expected_valid: int) -> None:
    counters = result.manifest["counters"]
    for rule in NEW_RULES:
        assert counters["rule_violations"][rule] == 0, rule
    assert counters["quarantined"] == 0, counters["quarantined_by_rule"]
    assert counters["valid_loaded"] == expected_valid
    assert counters["watermark_guarded"] == 0
    assert result.manifest["reconciliation"]["holds"]


def test_de02_baseline_output_passes_every_new_rule(tmp_path):
    run = run_incident_stream(baseline_incidents_config(), EVENTS)
    result = silver(bronze_from_lines(tmp_path / "bronze", run.lines))
    _assert_no_quarantine(result, EVENTS)


def test_de03_congestion_and_breakdown_output_passes_every_new_rule(tmp_path):
    whole_run = RecoveryWindow(start_tick=0, duration_ticks=EVENTS)
    config = baseline_incidents_config(
        congestion=CongestionConfig(enabled=True, route_id="ROUTE-01", window=whole_run),
        breakdown=BreakdownConfig(enabled=True, vehicle_ids=["VEH-0001", "VEH-0007"], window=whole_run),
    )
    run = run_incident_stream(config, EVENTS)
    payloads = [json.loads(line) for line in run.lines]
    # Guard: the scenarios really degraded the stream.
    assert any(p["status"] == "BREAKDOWN" for p in payloads)
    assert max(p["delay_seconds"] for p in payloads) > 240
    result = silver(bronze_from_lines(tmp_path / "bronze", run.lines))
    _assert_no_quarantine(result, EVENTS)


def test_de04_default_historical_telemetry_passes_every_new_rule(tmp_path):
    reference = generate_reference_dataset(load_scale_config())
    history = generate_batch_datasets(load_batch_config(), reference=reference).historical_telemetry
    assert len(history) > 1000
    fields = set(TelemetryEvent.model_fields)
    # The TelemetryEvent projection of each snapshot (the extra DE-04 columns
    # are not part of the streaming contract Silver validates).
    lines = [json.dumps(row.model_dump(mode="json", include=fields)) for row in history]
    # A batch load happens after the history it carries (1 day after the last snapshot).
    loaded_at = max(row.event_time for row in history).astimezone(UTC).replace(microsecond=0)
    clock = fixed_clock(datetime.fromtimestamp(loaded_at.timestamp() + 86400, tz=UTC))
    result = silver(bronze_from_lines(tmp_path / "bronze", lines, clock=clock), clock=clock)
    _assert_no_quarantine(result, len(history))
