"""GeneratorConfig loading tests: defaults are sane/externalized, and a
malformed config fails validation fast (mirrors
simulator/tests/test_config.py's pattern for DE-01's config)."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from simulator.generator.config import (
    DEFAULT_GENERATOR_CONFIG_PATH,
    load_generator_config,
)


def test_default_generator_config_path_exists():
    assert DEFAULT_GENERATOR_CONFIG_PATH.exists()


def test_default_generator_config_loads_with_sane_defaults():
    config = load_generator_config()
    assert config.events_per_second > 0
    assert config.cruise_speed_min_kmh <= config.cruise_speed_max_kmh
    assert 0.0 <= config.min_occupancy_fraction <= config.max_occupancy_fraction
    assert config.min_delay_seconds <= config.max_delay_seconds


def test_generator_config_is_externalized_not_hardcoded():
    """Changing the config file changes the loaded rate: proves the
    emission rate comes from configuration, not a hardcoded constant."""
    original = DEFAULT_GENERATOR_CONFIG_PATH.read_text()
    custom_text = original.replace(
        "events_per_second = 10.0", "events_per_second = 2.5"
    )
    assert custom_text != original

    with tempfile.TemporaryDirectory() as tmp:
        custom_path = Path(tmp) / "custom_generator.toml"
        custom_path.write_text(custom_text)
        custom_config = load_generator_config(custom_path)

    assert custom_config.events_per_second == 2.5
    assert load_generator_config().events_per_second == 10.0


def test_malformed_generator_config_fails_validation():
    with tempfile.TemporaryDirectory() as tmp:
        bad_path = Path(tmp) / "bad_generator.toml"
        # cruise_speed_min_kmh > cruise_speed_max_kmh must fail.
        bad_path.write_text(
            """
[generator]
seed = 1
events_per_second = 10.0
simulated_start_time = 2025-01-01T00:00:00Z
cruise_speed_min_kmh = 90.0
cruise_speed_max_kmh = 10.0
dwell_speed_jitter_kmh = 3.0
depart_probability_per_tick = 0.6
min_occupancy_fraction = 0.0
max_occupancy_fraction = 0.95
min_delay_seconds = -30
max_delay_seconds = 180
delayed_status_threshold_seconds = 90
log_level = "INFO"
"""
        )
        with pytest.raises(ValidationError):
            load_generator_config(bad_path)


def test_unknown_field_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        bad_path = Path(tmp) / "unknown_field.toml"
        bad_path.write_text(
            """
[generator]
seed = 1
events_per_second = 10.0
simulated_start_time = 2025-01-01T00:00:00Z
cruise_speed_min_kmh = 10.0
cruise_speed_max_kmh = 60.0
dwell_speed_jitter_kmh = 3.0
depart_probability_per_tick = 0.6
min_occupancy_fraction = 0.0
max_occupancy_fraction = 0.95
min_delay_seconds = -30
max_delay_seconds = 180
delayed_status_threshold_seconds = 90
log_level = "INFO"
not_a_real_field = 123
"""
        )
        with pytest.raises(ValidationError):
            load_generator_config(bad_path)
