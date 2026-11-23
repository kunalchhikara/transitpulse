"""Config-loading tests: counts match PROJECT.md section 5 defaults,
thresholds are externalized (loaded from file, not hardcoded), and
malformed config fails validation fast."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from simulator.config import load_scale_config, load_validation_thresholds
from simulator.config.loader import (
    DEFAULT_SCALE_CONFIG_PATH,
    DEFAULT_VALIDATION_CONFIG_PATH,
)


def test_default_scale_config_matches_project_targets():
    config = load_scale_config()
    assert config.counts.vehicle_count == 50
    assert config.counts.route_count == 10
    assert config.counts.depot_count == 4
    assert 100 <= config.counts.stop_count <= 200
    assert config.counts.stop_count_min == 100
    assert config.counts.stop_count_max == 200


def test_default_bounding_box_is_sydney_area():
    config = load_scale_config()
    bbox = config.sydney_bounding_box
    # Greater Sydney metro area, not the whole of NSW/Australia.
    assert -34.5 < bbox.min_latitude < bbox.max_latitude < -33.0
    assert 150.0 < bbox.min_longitude < bbox.max_longitude < 152.0


def test_validation_thresholds_loaded_from_config_file():
    thresholds = load_validation_thresholds()
    assert thresholds.max_speed_kmh == 120.0
    assert thresholds.future_time_tolerance_seconds == 120
    assert "1.0.0" in thresholds.supported_schema_versions
    assert "vehicle_id" in thresholds.required_fields
    assert "event_id" in thresholds.required_fields


def test_validation_thresholds_are_externalized_not_hardcoded():
    """Changing the config file changes the loaded thresholds: proves
    thresholds come from configuration, not from a hardcoded constant."""
    original = DEFAULT_VALIDATION_CONFIG_PATH.read_text()
    custom_text = original.replace("max_speed_kmh = 120.0", "max_speed_kmh = 55.0")
    assert custom_text != original

    with tempfile.TemporaryDirectory() as tmp:
        custom_path = Path(tmp) / "custom_validation.toml"
        custom_path.write_text(custom_text)
        custom_thresholds = load_validation_thresholds(custom_path)

    assert custom_thresholds.max_speed_kmh == 55.0
    default_thresholds = load_validation_thresholds()
    assert default_thresholds.max_speed_kmh == 120.0


def test_malformed_scale_config_fails_validation():
    with tempfile.TemporaryDirectory() as tmp:
        bad_path = Path(tmp) / "bad_scale.toml"
        # stop_count outside [stop_count_min, stop_count_max] must fail.
        bad_path.write_text(
            """
seed = 1

[counts]
vehicle_count = 50
route_count = 10
stop_count = 999
stop_count_min = 100
stop_count_max = 200
depot_count = 4

[sydney_bounding_box]
min_latitude = -34.05
max_latitude = -33.55
min_longitude = 150.55
max_longitude = 151.35

[fleet]
min_vehicle_capacity = 40
max_vehicle_capacity = 80

[route_shape]
min_stops_per_route = 8
max_stops_per_route = 18

[depot]
min_capacity = 20
max_capacity = 60
"""
        )
        with pytest.raises(ValidationError):
            load_scale_config(bad_path)


def test_default_scale_config_path_exists():
    assert DEFAULT_SCALE_CONFIG_PATH.exists()
