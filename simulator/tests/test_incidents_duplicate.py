"""PROJECT.md section 13 item 2: duplicate-event injection -- replay a
prior event_id verbatim."""

from __future__ import annotations

from simulator.incidents.config import DuplicateConfig
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import build_injector


def test_duplicate_disabled_never_repeats_an_event_id():
    injector = build_injector()
    event_ids = [injector.produce_event(tick)["event_id"] for tick in range(30)]
    assert len(event_ids) == len(set(event_ids))


def test_duplicate_enabled_replays_a_prior_payload_verbatim():
    """probability_per_tick=1.0 forces a replay on every tick once history
    is non-empty (tick 0 cannot replay anything -- there is no prior event
    yet)."""
    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))

    first = injector.produce_event(0)
    second = injector.produce_event(1)
    third = injector.produce_event(2)

    # tick 0: nothing to replay yet -> this tick's own content.
    assert second == first, "tick 1 must replay tick 0's payload verbatim"
    assert third in (first, second), "tick 2 must replay a payload already seen, verbatim"


def test_duplicate_replayed_payload_is_byte_identical_not_just_same_event_id():
    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    first = injector.produce_event(0)
    second = injector.produce_event(1)

    assert second["event_id"] == first["event_id"]
    # Every field, not merely event_id, is an exact (verbatim) copy.
    assert second == first


def test_duplicate_output_always_reproduces_an_already_generated_real_payload():
    """Every delivered payload, even when replayed, must be a verbatim copy
    of something the underlying generator had *already* produced by that
    point -- never a payload invented on the spot and never one "from the
    future" (a later tick's content arriving early)."""
    reference = build_generator()
    real_payloads = [reference.produce_event(tick).model_dump(mode="json") for tick in range(10)]

    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    delivered = [injector.produce_event(tick) for tick in range(10)]

    for i, payload in enumerate(delivered):
        assert payload in real_payloads[: i + 1]
