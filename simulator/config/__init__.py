"""Externalized configuration for the TransitPulse simulator.

All scale (entity counts, geographic bounds, fleet/route/depot shape) and
data-quality validation-threshold values live in the TOML files in this
package (`default_scale.toml`, `default_validation.toml`) rather than as
constants scattered through code, per `PROJECT.md` section 19 and the
DE-01 acceptance criteria.
"""

from simulator.config.loader import load_scale_config, load_validation_thresholds
from simulator.config.models import (
    BoundingBox,
    CountsConfig,
    DepotShapeConfig,
    FleetConfig,
    RouteShapeConfig,
    ScaleConfig,
    ValidationThresholds,
)

__all__ = [
    "BoundingBox",
    "CountsConfig",
    "DepotShapeConfig",
    "FleetConfig",
    "RouteShapeConfig",
    "ScaleConfig",
    "ValidationThresholds",
    "load_scale_config",
    "load_validation_thresholds",
]
