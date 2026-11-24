"""Emission-rate test: the runner paces output at the configured
`events_per_second`, verified with an injected `FakeClock` -- no real
`time.sleep` anywhere in this test, so it runs instantly and
deterministically (per this story's test requirements)."""

import pytest

from simulator.generator.clock import FakeClock
from simulator.generator.runner import run
from simulator.tests._generator_helpers import build_generator


def test_emission_rate_matches_configured_target_with_fake_clock():
    generator = build_generator()
    target_rate = generator.generator_config.events_per_second

    emitted = []
    clock = FakeClock()

    summary = run(generator, emitted.append, clock=clock, max_events=300)

    assert summary.events_emitted == 300
    assert len(emitted) == 300
    assert summary.elapsed_seconds > 0

    observed_rate = summary.events_emitted / summary.elapsed_seconds
    assert observed_rate == pytest.approx(target_rate, rel=0.01)


def test_emission_rate_scales_with_configured_target():
    """A different configured rate yields a proportionally different
    observed rate -- proves the rate is read from config, not hardcoded."""
    base_config = build_generator().generator_config
    faster_config = base_config.model_copy(
        update={"events_per_second": base_config.events_per_second * 4}
    )
    faster_generator = build_generator(generator_config=faster_config)

    emitted = []
    clock = FakeClock()
    summary = run(faster_generator, emitted.append, clock=clock, max_events=300)

    observed_rate = summary.events_emitted / summary.elapsed_seconds
    assert observed_rate == pytest.approx(faster_config.events_per_second, rel=0.01)


def test_should_stop_halts_run_before_max_events():
    generator = build_generator()
    emitted = []
    clock = FakeClock()

    def stop_after_ten():
        return len(emitted) >= 10

    summary = run(
        generator, emitted.append, clock=clock, max_events=10_000, should_stop=stop_after_ten
    )

    assert summary.stopped_early is True
    assert summary.events_emitted == 10
    assert len(emitted) == 10
