"""PROJECT.md section 13 item 5: route congestion -- lower speed and raise
delays on a route so its KPIs visibly degrade, then recover once the
configured window closes (section 13 item 8)."""

from __future__ import annotations

from simulator.incidents.config import CongestionConfig, RecoveryWindow
from simulator.schemas.telemetry import VehicleStatus
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector

_WINDOW_DURATION = 60
_TOTAL_TICKS = 150


def _target_route_id() -> str:
    """The route the vehicle visited at tick 0 is guaranteed to recur every
    `vehicle_count` ticks (round-robin scheduling), so congestion on it is
    observable without hardcoding a route_id the reference data may not
    actually assign."""
    reference = build_generator()
    return reference.produce_event(0).route_id


def test_congestion_degrades_speed_and_delay_only_on_target_route_within_window():
    route_id = _target_route_id()
    baseline = build_generator()
    injector = build_injector(
        congestion=CongestionConfig(
            enabled=True,
            route_id=route_id,
            window=RecoveryWindow(start_tick=0, duration_ticks=_WINDOW_DURATION),
            speed_multiplier=0.25,
            extra_delay_seconds=300,
        )
    )

    matched_target_route_at_least_once = False
    for tick in range(_WINDOW_DURATION):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)

        if baseline_event.route_id == route_id:
            matched_target_route_at_least_once = True
            assert payload["speed_kmh"] == round(baseline_event.speed_kmh * 0.25, 2)
            assert payload["delay_seconds"] == baseline_event.delay_seconds + 300
            assert payload["status"] == VehicleStatus.DELAYED.value
        else:
            # Vehicles on other routes are completely unaffected.
            assert payload == baseline_event.model_dump(mode="json")

    assert matched_target_route_at_least_once, "test setup must actually exercise the target route"


def test_congestion_recovers_to_exact_baseline_after_window_closes():
    route_id = _target_route_id()
    baseline = build_generator()
    injector = build_injector(
        congestion=CongestionConfig(
            enabled=True,
            route_id=route_id,
            window=RecoveryWindow(start_tick=0, duration_ticks=_WINDOW_DURATION),
            speed_multiplier=0.25,
            extra_delay_seconds=300,
        )
    )

    # Fast-forward both streams through the congestion window.
    for tick in range(_WINDOW_DURATION):
        baseline.produce_event(tick)
        injector.produce_event(tick)

    matched_target_route_at_least_once = False
    for tick in range(_WINDOW_DURATION, _TOTAL_TICKS):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)
        if baseline_event.route_id == route_id:
            matched_target_route_at_least_once = True
        # After the window: byte-identical to baseline, congested or not.
        assert payload == baseline_event.model_dump(mode="json")

    assert matched_target_route_at_least_once, "test setup must re-exercise the target route after recovery"


def test_congestion_disabled_matches_baseline_exactly():
    baseline = build_generator()
    injector = build_injector()
    for tick in range(20):
        assert injector.produce_event(tick) == baseline.produce_event(tick).model_dump(mode="json")
