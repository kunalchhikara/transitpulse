"""PROJECT.md section 13 item 3: late-event injection -- `event_time`
significantly earlier than arrival/`generated_at`, beyond a configurable
threshold."""

from __future__ import annotations

from datetime import datetime

from simulator.incidents.config import LateEventConfig
from simulator.tests._incident_helpers import build_injector


def test_late_event_disabled_leaves_event_time_equal_to_generated_at():
    injector = build_injector()
    for tick in range(10):
        payload = injector.produce_event(tick)
        assert payload["event_time"] == payload["generated_at"]


def test_late_event_enabled_skews_event_time_materially_earlier_than_threshold():
    threshold = 60
    injector = build_injector(
        late_event=LateEventConfig(
            enabled=True,
            probability_per_tick=1.0,
            min_skew_seconds=300,
            max_skew_seconds=300,
            late_threshold_seconds=threshold,
        )
    )

    for tick in range(5):
        payload = injector.produce_event(tick)
        generated_at = datetime.fromisoformat(payload["generated_at"])
        event_time = datetime.fromisoformat(payload["event_time"])
        skew = (generated_at - event_time).total_seconds()
        assert skew == 300
        assert skew > threshold, "skew must exceed the configured late threshold"


def test_late_event_skew_is_drawn_from_configured_range():
    injector = build_injector(
        late_event=LateEventConfig(
            enabled=True,
            probability_per_tick=1.0,
            min_skew_seconds=100,
            max_skew_seconds=200,
            late_threshold_seconds=60,
        )
    )
    skews = set()
    for tick in range(40):
        payload = injector.produce_event(tick)
        generated_at = datetime.fromisoformat(payload["generated_at"])
        event_time = datetime.fromisoformat(payload["event_time"])
        skew = (generated_at - event_time).total_seconds()
        assert 100 <= skew <= 200
        skews.add(skew)
    assert len(skews) > 1, "skew should vary within the configured range, not be a constant"


def test_late_event_config_requires_min_skew_at_least_threshold():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        LateEventConfig(enabled=True, min_skew_seconds=10, late_threshold_seconds=60)
