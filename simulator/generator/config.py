"""Generator-specific configuration: emission rate, movement/dwell dynamics,
normal-operation value ranges and the deterministic virtual-clock start
time this story's telemetry loop uses.

This is deliberately a *separate* config surface from
`simulator/config/` (DE-01's `ScaleConfig`/`ValidationThresholds`
contract, which this story does not modify) -- it is specific to DE-02's
telemetry-generation runtime, owned by this story, and kept config-driven
rather than hardcoded per `PROJECT.md` section 19.
"""

from __future__ import annotations

import tomllib
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

_GENERATOR_DIR = Path(__file__).parent
DEFAULT_GENERATOR_CONFIG_PATH = _GENERATOR_DIR / "default_generator.toml"


class GeneratorConfig(BaseModel):
    """Validated, config-driven settings for `TelemetryGenerator`/`run`."""

    model_config = {"extra": "forbid"}

    #: Seeds the single `random.Random` instance driving all generator
    #: randomness (movement/dwell jitter, occupancy, delay). Same seed +
    #: same config -> byte-identical event sequence.
    seed: int

    #: Target aggregate emission rate across all simulated vehicles
    #: (events/sec). Also used as the real-time pacing tick rate.
    events_per_second: float = Field(gt=0)

    #: Base timestamp for the deterministic *virtual* event-time timeline
    #: (`event_time`/`generated_at` = this + tick_index / events_per_second).
    #: Using a config value rather than wall-clock `datetime.now()` is what
    #: makes two separate runs byte-identical.
    simulated_start_time: datetime

    #: Normal cruising-speed range (km/h) a vehicle moves at between stops.
    cruise_speed_min_kmh: float = Field(ge=0.0)
    cruise_speed_max_kmh: float = Field(gt=0.0)

    #: Small nonzero speed jitter reported while a vehicle is dwelling at a
    #: stop (never exactly 0, which would be an unrealistic constant).
    dwell_speed_jitter_kmh: float = Field(ge=0.0)

    #: Probability, per tick, that a dwelling vehicle departs its current
    #: stop and resumes cruising toward the next one.
    depart_probability_per_tick: float = Field(ge=0.0, le=1.0)

    #: Occupancy reported as `round(vehicle.capacity * fraction)`, fraction
    #: sampled uniformly from this range.
    min_occupancy_fraction: float = Field(ge=0.0)
    max_occupancy_fraction: float = Field(ge=0.0)

    #: Normal-operation delay range (seconds); negative means early. This
    #: models ordinary schedule variance, not a DE-03 incident scenario.
    min_delay_seconds: int
    max_delay_seconds: int

    #: `status` is reported as DELAYED (rather than IN_SERVICE) once
    #: `delay_seconds` reaches this threshold. BREAKDOWN/OUT_OF_SERVICE are
    #: never produced by this story -- see DE-03.
    delayed_status_threshold_seconds: int

    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_ranges(self) -> GeneratorConfig:
        if self.cruise_speed_min_kmh > self.cruise_speed_max_kmh:
            raise ValueError("cruise_speed_min_kmh must be <= cruise_speed_max_kmh")
        if self.min_occupancy_fraction > self.max_occupancy_fraction:
            raise ValueError(
                "min_occupancy_fraction must be <= max_occupancy_fraction"
            )
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("min_delay_seconds must be <= max_delay_seconds")
        return self


def load_generator_config(path: str | Path | None = None) -> GeneratorConfig:
    """Load and validate the generator's runtime configuration.

    Args:
        path: Optional path to an alternate generator-config TOML file.
            Defaults to `simulator/generator/default_generator.toml`.
    """
    resolved = Path(path) if path is not None else DEFAULT_GENERATOR_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return GeneratorConfig.model_validate(data["generator"])
