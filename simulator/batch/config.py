"""DE-04 batch-dataset configuration: volume, date range, seed and value
ranges for the PROJECT.md section 7 batch sources.

Deliberately a separate config surface from DE-01's `simulator/config/`
(reference-entity scale, which this story imports but does not modify) and
DE-02's `simulator/generator/` (streaming runtime). Every behavioural number
used by `simulator/batch/` lives in `default_batch.toml`, is validated here
on load (fail fast on an inconsistent config), and can be overridden by
pointing `load_batch_config()` / the CLI at an alternate TOML file.
"""

from __future__ import annotations

import tomllib
from datetime import date, datetime, time, timedelta
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

_BATCH_DIR = Path(__file__).parent
DEFAULT_BATCH_CONFIG_PATH = _BATCH_DIR / "default_batch.toml"

_MINUTES_PER_DAY = 24 * 60


class Severity(str, Enum):
    """Service-disruption severity levels (`service_disruptions.severity`)."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class DisruptionEffect(str, Enum):
    """Implemented disruption behaviours. See `default_batch.toml`."""

    DELAY = "DELAY"
    BREAKDOWN = "BREAKDOWN"
    CANCEL = "CANCEL"


class _Strict(BaseModel):
    model_config = {"extra": "forbid"}


class GeneralConfig(_Strict):
    seed: int
    start_date: date
    service_days: int = Field(gt=0)
    utc_offset_hours: int = Field(ge=-12, le=14)

    @property
    def service_dates(self) -> list[date]:
        return [self.start_date + timedelta(days=i) for i in range(self.service_days)]


class SchedulesConfig(_Strict):
    service_start_time: time
    service_end_time: time
    headway_minutes: int = Field(gt=0)
    min_segment_runtime_seconds: int = Field(gt=0)
    max_segment_runtime_seconds: int = Field(gt=0)
    dwell_seconds: int = Field(ge=0)
    min_layover_minutes: int = Field(ge=0)
    publish_lead_days: int = Field(ge=0)

    @model_validator(mode="after")
    def _check(self) -> SchedulesConfig:
        if self.service_start_time >= self.service_end_time:
            raise ValueError("service_start_time must be before service_end_time")
        if self.min_segment_runtime_seconds > self.max_segment_runtime_seconds:
            raise ValueError(
                "min_segment_runtime_seconds must be <= max_segment_runtime_seconds"
            )
        return self

    @property
    def trips_per_route_per_day(self) -> int:
        """floor((end - start) / headway) + 1 departures per route per day."""
        anchor = date(2000, 1, 1)
        window = datetime.combine(anchor, self.service_end_time) - datetime.combine(
            anchor, self.service_start_time
        )
        return int(window.total_seconds() // (self.headway_minutes * 60)) + 1


class OperationsConfig(_Strict):
    min_start_delay_seconds: int
    max_start_delay_seconds: int
    min_segment_drift_seconds: int
    max_segment_drift_seconds: int
    min_delay_seconds: int
    delayed_status_threshold_seconds: int
    min_arrival_speed_kmh: float = Field(ge=0.0)
    max_arrival_speed_kmh: float = Field(ge=0.0)
    trip_record_settlement_minutes: int = Field(ge=0)
    disruption_record_settlement_minutes: int = Field(ge=0)

    @model_validator(mode="after")
    def _check(self) -> OperationsConfig:
        if self.min_start_delay_seconds > self.max_start_delay_seconds:
            raise ValueError("min_start_delay_seconds must be <= max_start_delay_seconds")
        if self.min_segment_drift_seconds > self.max_segment_drift_seconds:
            raise ValueError(
                "min_segment_drift_seconds must be <= max_segment_drift_seconds"
            )
        if self.min_arrival_speed_kmh > self.max_arrival_speed_kmh:
            raise ValueError("min_arrival_speed_kmh must be <= max_arrival_speed_kmh")
        if self.min_delay_seconds > self.min_start_delay_seconds:
            raise ValueError("min_delay_seconds must be <= min_start_delay_seconds")
        return self


class SeverityConfig(_Strict):
    weight: int = Field(ge=0)
    min_delay_impact_seconds: int = Field(ge=0)
    max_delay_impact_seconds: int = Field(ge=0)

    @model_validator(mode="after")
    def _check(self) -> SeverityConfig:
        if self.min_delay_impact_seconds > self.max_delay_impact_seconds:
            raise ValueError(
                "min_delay_impact_seconds must be <= max_delay_impact_seconds"
            )
        return self


class DisruptionTypeConfig(_Strict):
    name: str = Field(min_length=1)
    effect: DisruptionEffect
    weight: int = Field(ge=0)
    min_duration_minutes: int = Field(gt=0)
    max_duration_minutes: int = Field(gt=0)

    @model_validator(mode="after")
    def _check(self) -> DisruptionTypeConfig:
        if self.min_duration_minutes > self.max_duration_minutes:
            raise ValueError("min_duration_minutes must be <= max_duration_minutes")
        return self


class DisruptionsConfig(_Strict):
    per_day: int = Field(ge=0)
    severities: dict[Severity, SeverityConfig]
    types: list[DisruptionTypeConfig]

    @model_validator(mode="after")
    def _check(self) -> DisruptionsConfig:
        if self.per_day > 0:
            if not self.types or sum(t.weight for t in self.types) <= 0:
                raise ValueError("at least one disruption type needs a positive weight")
            if not self.severities or sum(s.weight for s in self.severities.values()) <= 0:
                raise ValueError("at least one severity needs a positive weight")
        names = [t.name for t in self.types]
        if len(names) != len(set(names)):
            raise ValueError("disruption type names must be unique")
        return self


class FareTypeConfig(_Strict):
    weight: int = Field(ge=0)
    fare_aud: float = Field(ge=0.0)


class PassengersConfig(_Strict):
    min_journeys_per_trip: int = Field(ge=0)
    max_journeys_per_trip: int = Field(ge=0)
    card_population: int = Field(gt=0)
    settlement_lag_minutes: int = Field(ge=0)
    fare_types: dict[str, FareTypeConfig]

    @model_validator(mode="after")
    def _check(self) -> PassengersConfig:
        if self.min_journeys_per_trip > self.max_journeys_per_trip:
            raise ValueError("min_journeys_per_trip must be <= max_journeys_per_trip")
        if not self.fare_types or sum(f.weight for f in self.fare_types.values()) <= 0:
            raise ValueError("at least one fare type needs a positive weight")
        return self


class WeatherConfig(_Strict):
    enabled: bool
    observation_interval_minutes: int = Field(gt=0)
    base_temperature_c: float
    diurnal_amplitude_c: float = Field(ge=0.0)
    peak_temperature_hour: int = Field(ge=0, le=23)
    temperature_noise_c: float = Field(ge=0.0)
    rain_probability: float = Field(ge=0.0, le=1.0)
    max_rainfall_mm: float = Field(ge=0.0)
    heavy_rain_threshold_mm: float = Field(ge=0.0)
    max_wind_speed_kmh: float = Field(ge=0.0)
    storm_wind_threshold_kmh: float = Field(ge=0.0)
    cloudy_probability: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _check(self) -> WeatherConfig:
        if _MINUTES_PER_DAY % self.observation_interval_minutes != 0:
            raise ValueError("observation_interval_minutes must divide 1440")
        return self

    @property
    def observations_per_day(self) -> int:
        return _MINUTES_PER_DAY // self.observation_interval_minutes


class BatchConfig(_Strict):
    """Top-level validated DE-04 configuration."""

    batch: GeneralConfig
    schedules: SchedulesConfig
    operations: OperationsConfig
    disruptions: DisruptionsConfig
    passengers: PassengersConfig
    weather: WeatherConfig

    @model_validator(mode="after")
    def _check_monotonic_times(self) -> BatchConfig:
        # Guarantees actual arrival at stop k+1 is strictly after actual
        # departure from stop k even under the most negative drift.
        if (
            self.schedules.min_segment_runtime_seconds
            + self.operations.min_segment_drift_seconds
            <= 0
        ):
            raise ValueError(
                "min_segment_runtime_seconds + min_segment_drift_seconds must be > 0 "
                "so actual stop times stay strictly increasing"
            )
        return self

    def with_seed(self, seed: int) -> BatchConfig:
        """Return a copy with `batch.seed` replaced (CLI --seed override)."""
        return self.model_copy(
            update={"batch": self.batch.model_copy(update={"seed": seed})}
        )


def load_batch_config(path: str | Path | None = None) -> BatchConfig:
    """Load and validate the DE-04 batch configuration.

    Args:
        path: Optional path to an alternate batch-config TOML file. Defaults
            to `simulator/batch/default_batch.toml`.
    """
    resolved = Path(path) if path is not None else DEFAULT_BATCH_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return BatchConfig.model_validate(data)
