"""Determinism contract (DE-01/DE-02 consistency): same seed + config ->
same incident timing/content, both for the underlying generator and for
every DE-03 incident decision -- verified across two separate, independent
`IncidentInjector` instances built from identical config."""

from __future__ import annotations

import json

from simulator.incidents.config import (
    CongestionConfig,
    DuplicateConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
)
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import baseline_incidents_config, build_injector

_EVENT_COUNT = 150


def _mixed_incidents_config():
    return baseline_incidents_config(
        malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.2),
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.2),
        late_event=LateEventConfig(enabled=True, probability_per_tick=0.2),
        out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.2, hold_ticks=3),
        congestion=CongestionConfig(
            enabled=True, route_id="ROUTE-01", window=RecoveryWindow(start_tick=10, duration_ticks=50)
        ),
    )


def test_same_seed_and_config_produce_byte_identical_incident_sequences():
    config = _mixed_incidents_config()
    injector_a = build_injector(generator=build_generator(), incidents_config=config)
    injector_b = build_injector(generator=build_generator(), incidents_config=config)

    sequence_a = [injector_a.produce_event(t) for t in range(_EVENT_COUNT)]
    sequence_b = [injector_b.produce_event(t) for t in range(_EVENT_COUNT)]
    sequence_a += injector_a.flush_pending()
    sequence_b += injector_b.flush_pending()

    dumped_a = [json.dumps(p, sort_keys=True, default=str) for p in sequence_a]
    dumped_b = [json.dumps(p, sort_keys=True, default=str) for p in sequence_b]

    assert dumped_a, "no events were produced"
    assert dumped_a == dumped_b


def test_different_incidents_seed_produces_a_different_sequence():
    config_a = _mixed_incidents_config()
    config_b = config_a.model_copy(update={"seed": config_a.seed + 1})

    injector_a = build_injector(generator=build_generator(), incidents_config=config_a)
    injector_b = build_injector(generator=build_generator(), incidents_config=config_b)

    sequence_a = [injector_a.produce_event(t) for t in range(_EVENT_COUNT)]
    sequence_b = [injector_b.produce_event(t) for t in range(_EVENT_COUNT)]

    dumped_a = [json.dumps(p, sort_keys=True, default=str) for p in sequence_a]
    dumped_b = [json.dumps(p, sort_keys=True, default=str) for p in sequence_b]
    assert dumped_a != dumped_b


def test_enabling_an_incident_does_not_change_the_underlying_generator_seed_requirement():
    """Two injectors built from generators with the *same* seed but
    *different* incidents configs (one with everything disabled) must
    agree on every tick where no incident actually fired -- proving
    incident decisions are drawn from a separate RNG stream and never
    perturb the underlying deterministic generator content itself."""
    plain_injector = build_injector(generator=build_generator())
    incident_injector = build_injector(
        generator=build_generator(),
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.0),
    )

    for tick in range(30):
        assert plain_injector.produce_event(tick) == incident_injector.produce_event(tick)
