"""PROJECT.md section 13 item 4: out-of-order delivery."""

from __future__ import annotations

from simulator.incidents.config import OutOfOrderConfig
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector


def _expected_event_ids(total_ticks: int) -> list[str]:
    reference = build_generator()
    return [reference.produce_event(tick).event_id for tick in range(total_ticks)]


def test_out_of_order_disabled_preserves_generation_order():
    injector = build_injector()
    delivered_ids = [injector.produce_event(tick)["event_id"] for tick in range(20)]
    assert delivered_ids == _expected_event_ids(20)


def test_out_of_order_enabled_delivers_a_permutation_not_the_generation_order():
    """probability_per_tick=1.0 forces a reorder release on every call once
    the buffer is warm; the full delivered sequence (including the tail
    drained by `flush_pending`) must be exactly the same *set* of events as
    generation order -- nothing lost, nothing duplicated -- but delivered
    in a different *order*, which is the out-of-order anomaly itself."""
    hold_ticks = 3
    calls = 20
    total_ticks = calls + hold_ticks  # hold_ticks are pre-filled at construction

    injector = build_injector(
        out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=1.0, hold_ticks=hold_ticks)
    )
    delivered = [injector.produce_event(tick)["event_id"] for tick in range(calls)]
    delivered += [p["event_id"] for p in injector.flush_pending()]

    expected = _expected_event_ids(total_ticks)

    assert sorted(delivered) == sorted(expected), "no event may be lost or duplicated"
    assert delivered != expected, "delivery order must differ from generation order"


def test_out_of_order_disabled_by_default_never_reorders():
    injector = build_injector()
    assert injector.flush_pending() == []
