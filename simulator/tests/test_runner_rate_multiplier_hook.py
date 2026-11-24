"""Direct unit coverage for the one small, additive DE-03 hook added to
`simulator/generator/runner.py`: the optional `rate_multiplier` parameter.
Exercised here independently of `IncidentInjector` to isolate "did the hook
itself land correctly" from "does the incidents package use it correctly"
(the latter is covered by `simulator/tests/test_incidents_volume_spike.py`).
"""

from __future__ import annotations

import pytest

from simulator.generator.clock import FakeClock
from simulator.generator.runner import run
from simulator.tests._generator_helpers import build_generator


def test_rate_multiplier_omitted_matches_prior_unmodified_behavior():
    generator = build_generator()
    emitted = []
    summary = run(generator, emitted.append, clock=FakeClock(), max_events=50)
    expected_elapsed = 50 / generator.generator_config.events_per_second
    assert summary.elapsed_seconds == pytest.approx(expected_elapsed, rel=1e-9)


def test_rate_multiplier_constant_one_matches_omitted_case():
    generator = build_generator()
    emitted = []
    summary = run(generator, emitted.append, clock=FakeClock(), max_events=50, rate_multiplier=lambda _t: 1.0)
    expected_elapsed = 50 / generator.generator_config.events_per_second
    assert summary.elapsed_seconds == pytest.approx(expected_elapsed, rel=1e-9)


def test_rate_multiplier_speeds_up_only_the_ticks_it_applies_to():
    generator = build_generator()
    base_rate = generator.generator_config.events_per_second
    emitted = []

    def multiplier(tick: int) -> float:
        return 4.0 if tick < 20 else 1.0

    summary = run(generator, emitted.append, clock=FakeClock(), max_events=50, rate_multiplier=multiplier)

    expected = (20 / (base_rate * 4.0)) + (30 / base_rate)
    assert summary.elapsed_seconds == pytest.approx(expected, rel=1e-9)
