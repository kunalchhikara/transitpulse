"""PROJECT.md section 13 item 8: recovery -- after an incident's configured
duration/trigger, behaviour reverts to exactly baseline. This is the
standalone recovery demonstration (distinct from the individual
congestion/breakdown/volume-spike tests, which each also check their own
recovery as part of their own scenario coverage): it exercises *two*
duration-windowed scenarios closing on the same boundary at once, proving
recovery is not an artifact of only one scenario being active."""

from __future__ import annotations

from simulator.incidents.config import BreakdownConfig, CongestionConfig, RecoveryWindow
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector

_WINDOW_DURATION = 40


def test_multiple_windowed_incidents_all_revert_to_exact_baseline_together():
    reference = build_generator()
    route_id = reference.produce_event(0).route_id
    vehicle_id = reference.produce_event(1).vehicle_id

    baseline = build_generator()
    injector = build_injector(
        congestion=CongestionConfig(
            enabled=True,
            route_id=route_id,
            window=RecoveryWindow(start_tick=0, duration_ticks=_WINDOW_DURATION),
        ),
        breakdown=BreakdownConfig(
            enabled=True,
            vehicle_ids=[vehicle_id],
            window=RecoveryWindow(start_tick=0, duration_ticks=_WINDOW_DURATION),
        ),
    )

    any_anomaly_seen_during_window = False
    for tick in range(_WINDOW_DURATION):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)
        if payload != baseline_event.model_dump(mode="json"):
            any_anomaly_seen_during_window = True
    assert any_anomaly_seen_during_window, "test setup must actually exercise at least one incident"

    any_target_recurs_after_window = False
    for tick in range(_WINDOW_DURATION, _WINDOW_DURATION + 100):
        baseline_event = baseline.produce_event(tick)
        payload = injector.produce_event(tick)
        # Recovery: every field, for every vehicle/route, byte-identical to
        # a plain no-incident baseline run sharing the same seed/config.
        assert payload == baseline_event.model_dump(mode="json")
        if baseline_event.route_id == route_id or baseline_event.vehicle_id == vehicle_id:
            any_target_recurs_after_window = True

    assert any_target_recurs_after_window, "targets must recur post-window to prove recovery, not mere absence"
