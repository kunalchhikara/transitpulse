"""Loaders that read TOML configuration files into validated Pydantic models.

Python 3.11's standard-library `tomllib` is used for parsing (read-only,
no extra dependency). Falling back to a bundled default file keeps
`load_scale_config()` / `load_validation_thresholds()` usable with zero
arguments while still allowing any caller (tests, future CLI, future
Silver-layer code) to point at an alternate config file to rescale or
retune thresholds without code changes.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from simulator.config.models import ScaleConfig, ValidationThresholds

_CONFIG_DIR = Path(__file__).parent
DEFAULT_SCALE_CONFIG_PATH = _CONFIG_DIR / "default_scale.toml"
DEFAULT_VALIDATION_CONFIG_PATH = _CONFIG_DIR / "default_validation.toml"


def _read_toml(path: Path) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def load_scale_config(path: str | Path | None = None) -> ScaleConfig:
    """Load and validate the simulator's scale configuration.

    Args:
        path: Optional path to an alternate scale-config TOML file. Defaults
            to `simulator/config/default_scale.toml`.
    """
    resolved = Path(path) if path is not None else DEFAULT_SCALE_CONFIG_PATH
    data = _read_toml(resolved)
    return ScaleConfig.model_validate(data)


def load_validation_thresholds(path: str | Path | None = None) -> ValidationThresholds:
    """Load and validate the simulator's data-quality validation thresholds.

    Args:
        path: Optional path to an alternate validation-config TOML file.
            Defaults to `simulator/config/default_validation.toml`.
    """
    resolved = Path(path) if path is not None else DEFAULT_VALIDATION_CONFIG_PATH
    data = _read_toml(resolved)
    return ValidationThresholds.model_validate(data["telemetry"])
