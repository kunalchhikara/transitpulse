"""DE-04 config: the bundled default loads/validates, derived volume
properties are correct, and inconsistent configs fail fast on load."""

from datetime import date, time

import pytest
from pydantic import ValidationError

from simulator.batch.config import (
    DEFAULT_BATCH_CONFIG_PATH,
    BatchConfig,
    load_batch_config,
)
from simulator.tests._batch_helpers import SAMPLE_BATCH_CONFIG, with_overrides


def test_default_config_loads_with_expected_shape():
    cfg = load_batch_config()
    assert DEFAULT_BATCH_CONFIG_PATH.name == "default_batch.toml"
    assert cfg.batch.start_date == date(2025, 1, 6)
    assert cfg.batch.service_days == 7
    assert cfg.schedules.service_start_time == time(6, 0)
    # 06:00..22:00 every 60 min inclusive -> 17 departures per route per day.
    assert cfg.schedules.trips_per_route_per_day == 17
    assert cfg.weather.observations_per_day == 24
    assert len(cfg.batch.service_dates) == 7
    assert {t.effect.value for t in cfg.disruptions.types} == {"DELAY", "BREAKDOWN", "CANCEL"}


def test_sample_config_loads():
    cfg = load_batch_config(SAMPLE_BATCH_CONFIG)
    assert cfg.batch.service_days == 1
    assert cfg.schedules.trips_per_route_per_day == 6  # 06:00..21:00 every 3h


def test_with_seed_only_changes_seed():
    cfg = load_batch_config()
    reseeded = cfg.with_seed(cfg.batch.seed + 1)
    assert reseeded.batch.seed == cfg.batch.seed + 1
    assert reseeded.model_dump(exclude={"batch"}) == cfg.model_dump(exclude={"batch"})


@pytest.mark.parametrize(
    "overrides",
    [
        {"schedules": {"service_start_time": time(22, 0), "service_end_time": time(6, 0)}},
        {"schedules": {"min_segment_runtime_seconds": 500, "max_segment_runtime_seconds": 100}},
        {"schedules": {"headway_minutes": 0}},
        {"batch": {"service_days": 0}},
        {"batch": {"utc_offset_hours": 20}},
        {"operations": {"min_start_delay_seconds": 100, "max_start_delay_seconds": 0}},
        {"operations": {"min_delay_seconds": 0}},  # must be <= min_start_delay_seconds (-60)
        {"passengers": {"min_journeys_per_trip": 10, "max_journeys_per_trip": 1}},
        {"passengers": {"fare_types": {}}},
        {"weather": {"observation_interval_minutes": 7}},  # does not divide 1440
        {"weather": {"rain_probability": 1.5}},
        {"disruptions": {"types": []}},
        # Strictly-increasing actual stop times would not be guaranteed.
        {"schedules": {"min_segment_runtime_seconds": 20}, "operations": {"min_segment_drift_seconds": -20}},
    ],
)
def test_inconsistent_config_is_rejected(overrides):
    with pytest.raises(ValidationError):
        with_overrides(load_batch_config(), **overrides)


def test_unknown_disruption_effect_and_duplicate_names_rejected():
    base = load_batch_config()
    types = [t.model_dump(mode="python") for t in base.disruptions.types]
    bad_effect = [dict(types[0], effect="TELEPORT")]
    with pytest.raises(ValidationError):
        with_overrides(base, disruptions={"types": bad_effect})
    duplicated = [types[0], dict(types[1], name=types[0]["name"])]
    with pytest.raises(ValidationError):
        with_overrides(base, disruptions={"types": duplicated})


def test_unknown_keys_are_rejected():
    data = load_batch_config().model_dump(mode="python")
    data["batch"]["vehicle_count"] = 99  # belongs to DE-01 scale config, not here
    with pytest.raises(ValidationError):
        BatchConfig.model_validate(data)
