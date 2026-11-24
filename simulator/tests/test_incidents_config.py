"""DE-03 config loading/validation: `simulator/incidents/config.py` and the
bundled `default_incidents.toml`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    IncidentsConfig,
    LateEventConfig,
    MalformedDataConfig,
    load_incidents_config,
)


def test_default_bundled_config_loads_with_every_scenario_disabled():
    config = load_incidents_config()
    assert isinstance(config, IncidentsConfig)
    assert config.malformed.enabled is False
    assert config.duplicate.enabled is False
    assert config.late_event.enabled is False
    assert config.out_of_order.enabled is False
    assert config.congestion.enabled is False
    assert config.volume_spike.enabled is False
    assert config.breakdown.enabled is False


def test_config_rejects_unknown_top_level_field():
    with pytest.raises(ValidationError):
        IncidentsConfig(seed=1, not_a_real_field=True)


def test_congestion_requires_route_id_when_enabled():
    with pytest.raises(ValidationError):
        CongestionConfig(enabled=True, route_id=None)


def test_breakdown_requires_vehicle_ids_when_enabled():
    with pytest.raises(ValidationError):
        BreakdownConfig(enabled=True, vehicle_ids=[])


def test_malformed_requires_nonempty_modes():
    with pytest.raises(ValidationError):
        MalformedDataConfig(enabled=True, modes=[])


def test_late_event_requires_min_le_max_skew():
    with pytest.raises(ValidationError):
        LateEventConfig(enabled=True, min_skew_seconds=500, max_skew_seconds=100, late_threshold_seconds=60)


def test_everything_disabled_is_the_pydantic_default_with_no_arguments():
    config = IncidentsConfig()
    assert config.malformed.enabled is False
    assert config.breakdown.enabled is False
