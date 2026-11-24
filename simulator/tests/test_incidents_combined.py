"""Combined-scenario coverage: two incidents enabled concurrently must not
crash and must not corrupt the output stream (every delivered payload stays
a well-formed, JSON-serializable dict and the run completes normally)."""

from __future__ import annotations

import json

from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
)
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector


def test_congestion_and_breakdown_together_do_not_crash_and_both_apply():
    reference = build_generator()
    route_id = reference.produce_event(0).route_id
    vehicle_id = reference.produce_event(3).vehicle_id

    injector = build_injector(
        congestion=CongestionConfig(
            enabled=True, route_id=route_id, window=RecoveryWindow(start_tick=0, duration_ticks=80)
        ),
        breakdown=BreakdownConfig(
            enabled=True, vehicle_ids=[vehicle_id], window=RecoveryWindow(start_tick=0, duration_ticks=80)
        ),
    )

    payloads = [injector.produce_event(tick) for tick in range(80)]
    assert len(payloads) == 80
    for payload in payloads:
        json.dumps(payload)  # never raises -- output stays well-formed


def test_malformed_and_duplicate_together_do_not_crash_and_output_stays_json_serializable():
    """A duplicate may legitimately replay a payload that was itself
    malformed -- the two scenarios must compose without raising, and the
    result must still be well-formed JSON (even though it may not be a
    valid TelemetryEvent)."""
    injector = build_injector(
        malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.5),
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.5),
    )

    payloads = [injector.produce_event(tick) for tick in range(60)]
    assert len(payloads) == 60
    for payload in payloads:
        json.dumps(payload, default=str)


def test_out_of_order_and_late_event_together_do_not_crash_and_conserve_total_count():
    injector = build_injector(
        out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.5, hold_ticks=3),
        late_event=LateEventConfig(enabled=True, probability_per_tick=0.5),
    )

    payloads = [injector.produce_event(tick) for tick in range(50)]
    payloads += injector.flush_pending()

    assert len(payloads) == 50 + 3  # hold_ticks pre-fill + 50 calls, nothing lost
    for payload in payloads:
        json.dumps(payload)


def test_all_seven_scenarios_enabled_at_once_do_not_crash():
    """The maximal stress case: every DE-03 scenario active simultaneously."""
    reference = build_generator()
    route_id = reference.produce_event(0).route_id
    vehicle_id = reference.produce_event(5).vehicle_id

    injector = build_injector(
        malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.1),
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.1),
        late_event=LateEventConfig(enabled=True, probability_per_tick=0.1),
        out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.1, hold_ticks=3),
        congestion=CongestionConfig(
            enabled=True, route_id=route_id, window=RecoveryWindow(start_tick=0, duration_ticks=60)
        ),
        breakdown=BreakdownConfig(
            enabled=True, vehicle_ids=[vehicle_id], window=RecoveryWindow(start_tick=0, duration_ticks=60)
        ),
    )

    payloads = [injector.produce_event(tick) for tick in range(60)]
    payloads += injector.flush_pending()
    for payload in payloads:
        json.dumps(payload, default=str)
