"""PROJECT.md section 13 item 6: volume spike -- temporarily multiply the
configured emission rate for a duration, then return to baseline (section
13 item 8). Verified with `FakeClock` -- no real `time.sleep`, matching
DE-02's rate-test pattern (`simulator/tests/test_emission_rate.py`)."""

from __future__ import annotations

import pytest

from simulator.generator.clock import FakeClock
from simulator.generator.runner import run
from simulator.incidents.config import RecoveryWindow, VolumeSpikeConfig
from simulator.tests._incident_helpers import build_injector


def test_volume_spike_multiplies_rate_for_its_window_then_recovers_to_baseline():
    spike_duration = 50
    post_spike = 50
    multiplier = 5.0

    injector = build_injector(
        volume_spike=VolumeSpikeConfig(
            enabled=True,
            window=RecoveryWindow(start_tick=0, duration_ticks=spike_duration),
            rate_multiplier=multiplier,
        )
    )
    base_rate = injector.generator_config.events_per_second

    emitted = []
    clock = FakeClock()
    summary = run(
        injector,
        emitted.append,
        clock=clock,
        max_events=spike_duration + post_spike,
        rate_multiplier=injector.rate_multiplier,
    )

    assert summary.events_emitted == spike_duration + post_spike

    expected_elapsed = (spike_duration / (base_rate * multiplier)) + (post_spike / base_rate)
    assert summary.elapsed_seconds == pytest.approx(expected_elapsed, rel=1e-6)

    # The *observed* rate over the whole run must be faster than baseline
    # (because of the spike segment) but strictly slower than the full
    # spike multiplier (because the post-spike segment pulls it back down
    # to baseline) -- i.e. recovery genuinely happened, it isn't still
    # spiking for the rest of the run.
    observed_rate = summary.events_emitted / summary.elapsed_seconds
    assert base_rate < observed_rate < base_rate * multiplier


def test_volume_spike_disabled_matches_baseline_rate():
    injector = build_injector()
    base_rate = injector.generator_config.events_per_second

    emitted = []
    clock = FakeClock()
    summary = run(injector, emitted.append, clock=clock, max_events=100, rate_multiplier=injector.rate_multiplier)

    observed_rate = summary.events_emitted / summary.elapsed_seconds
    assert observed_rate == pytest.approx(base_rate, rel=0.01)


def test_volume_spike_recovery_segment_matches_a_plain_runner_run_exactly():
    """After the spike window, pacing is indistinguishable from a plain
    `run()` call with no rate_multiplier at all."""
    injector = build_injector(
        volume_spike=VolumeSpikeConfig(
            enabled=True,
            window=RecoveryWindow(start_tick=0, duration_ticks=10),
            rate_multiplier=8.0,
        )
    )
    base_rate = injector.generator_config.events_per_second

    emitted = []
    clock = FakeClock()
    run(injector, emitted.append, clock=clock, max_events=10, rate_multiplier=injector.rate_multiplier)
    elapsed_after_spike = clock.monotonic()

    # From here on the window is closed; 40 more events at plain baseline
    # pacing should take exactly 40 / base_rate seconds.
    run(injector, emitted.append, clock=clock, max_events=40, rate_multiplier=injector.rate_multiplier)
    recovered_segment_elapsed = clock.monotonic() - elapsed_after_spike

    assert recovered_segment_elapsed == pytest.approx(40 / base_rate, rel=1e-6)
