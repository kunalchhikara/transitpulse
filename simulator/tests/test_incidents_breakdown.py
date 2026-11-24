"""PROJECT.md section 13 item 7: vehicle breakdown -- status becomes
BREAKDOWN for the configured vehicle(s), for a configured recovery window
(section 13 item 8). Creating a downstream service-incident record is
explicitly out of DE-03's scope (see `simulator/incidents/config.py`
`BreakdownConfig` docstring)."""

from __future__ import annotations

from simulator.incidents.config import BreakdownConfig, RecoveryWindow
from simulator.schemas.telemetry import VehicleStatus
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector


def _target_vehicle_id() -> str:
    reference = build_generator()
    return reference.produce_event(0).vehicle_id


def test_breakdown_forces_status_and_zero_speed_for_target_vehicle_within_window():
    vehicle_id = _target_vehicle_id()
    baseline = build_generator()
    injector = build_injector(
        breakdown=BreakdownConfig(
            enabled=True,
            vehicle_ids=[vehicle_id],
            window=RecoveryWindow(start_tick=0, duration_ticks=50),
        )
    )

    matched_at_least_once = False
    for tick in range(50):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)
        if baseline_event.vehicle_id == vehicle_id:
            matched_at_least_once = True
            assert payload["status"] == VehicleStatus.BREAKDOWN.value
            assert payload["speed_kmh"] == 0.0
        else:
            assert payload == baseline_event.model_dump(mode="json")

    assert matched_at_least_once


def test_breakdown_recovers_to_exact_baseline_after_window_closes():
    vehicle_id = _target_vehicle_id()
    baseline = build_generator()
    injector = build_injector(
        breakdown=BreakdownConfig(
            enabled=True,
            vehicle_ids=[vehicle_id],
            window=RecoveryWindow(start_tick=0, duration_ticks=50),
        )
    )

    for tick in range(50):
        baseline.produce_event(tick)
        injector.produce_event(tick)

    matched_at_least_once = False
    for tick in range(50, 150):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)
        if baseline_event.vehicle_id == vehicle_id:
            matched_at_least_once = True
        assert payload == baseline_event.model_dump(mode="json")

    assert matched_at_least_once, "target vehicle must recur after recovery to prove it is no longer broken down"


def test_breakdown_disabled_never_reports_breakdown_status():
    injector = build_injector()
    statuses = {injector.produce_event(tick)["status"] for tick in range(100)}
    assert VehicleStatus.BREAKDOWN.value not in statuses
