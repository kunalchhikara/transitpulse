"""DE-06 Silver configuration (`default_silver.toml`).

A separate config surface from DE-01..DE-05 (imported, never modified).
Validated on load with pydantic (unknown keys rejected), so a typo in a
policy name fails fast instead of silently falling back.

Validation thresholds are deliberately *not* part of this model: they are
DE-01's `ValidationThresholds` (`simulator/config/default_validation.toml`),
loaded alongside this config, so PROJECT.md section 12 values have a single
source of truth.
"""

from __future__ import annotations

import tomllib
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator

from simulator.bronze.config import TABLE_NAME_PATTERN

DEFAULT_SILVER_CONFIG_PATH = Path(__file__).parent / "default_silver.toml"


class FutureTimeReference(str, Enum):
    """What `future_time_tolerance_seconds` is measured against. See README
    "DE-06: Future-time reference" for the rationale."""

    GENERATED_AT = "generated_at"
    INGESTION_TIME = "ingestion_time"


class WatermarkGuardMode(str, Enum):
    """What the DE-08 / FU-06 stream-relative watermark guard does with a
    valid event that jumps ahead of the stream without corroboration (README
    "DE-06: Watermark guard")."""

    FLAG = "flag"  # load it, `_silver_watermark_guarded = true`, never advances the watermark (default)
    OFF = "off"  # no guard (pre-DE-08 behaviour)


class WatermarkGuardConfig(BaseModel):
    """Stream-relative: decided only from the record stream in arrival order,
    never from a wall, ingestion or processing clock."""

    model_config = {"extra": "forbid"}

    mode: WatermarkGuardMode
    #: An event more than this far ahead of the current maximum event_time is
    #: a "jump" that must not be contradicted by the stream that follows it.
    max_jump_seconds: int = Field(gt=0)
    #: How many later valid, first-seen events are examined as evidence.
    lookahead_events: int = Field(ge=1)
    #: A jump is corroborated by at least this many DISTINCT other vehicles
    #: whose later events are no more than max_jump_seconds behind it; so up
    #: to this many clustered outliers from distinct vehicles (and any number
    #: from one vehicle) cannot corroborate each other.
    min_corroborating_vehicles: int = Field(ge=1)
    #: An uncorroborated jump is held back only if later events from at least
    #: this many DISTINCT vehicles (its own included) contradict it (are more
    #: than max_jump_seconds behind it). Evidence from one vehicle alone
    #: never decides: "one reading ahead, then that vehicle behind it" is
    #: ambiguous (a glitch, or genuinely late readings) and keeps main's result.
    min_contradicting_vehicles: int = Field(ge=1)

    @model_validator(mode="after")
    def _check_lookahead(self) -> WatermarkGuardConfig:
        if max(self.min_corroborating_vehicles, self.min_contradicting_vehicles) > self.lookahead_events:
            raise ValueError("min_corroborating_vehicles and min_contradicting_vehicles must be <= lookahead_events")
        return self


class ArrivalTimeSkewMode(str, Enum):
    """The optional, clock-relative `arrival_time_skew` rule."""

    OFF = "off"  # not evaluated (default)
    QUARANTINE = "quarantine"  # reject under rule `arrival_time_skew`


class ArrivalTimeReference(str, Enum):
    """The arrival instant `arrival_time_skew` measures event_time against."""

    INGESTION_TIME = "ingestion_time"  # Bronze ingestion_time of the record (recorded, replayable)
    PROCESSING_TIME = "processing_time"  # this Silver run's processing_time


class ArrivalTimeSkewConfig(BaseModel):
    """Opt-in, for live feeds whose producers share the ingestion clock:
    `event_time <= arrival + max_skew_seconds`. Wrong for simulated/replayed
    data (simulated time is unrelated to the ingestion clock), hence `off` by
    default."""

    model_config = {"extra": "forbid"}

    mode: ArrivalTimeSkewMode
    reference: ArrivalTimeReference
    max_skew_seconds: int = Field(ge=0)


class SilverConfig(BaseModel):
    model_config = {"extra": "forbid"}

    output_dir: Path
    bronze_dir: Path
    bronze_table: str
    clean_table: str
    quarantine_table: str
    duplicates_table: str
    allowed_lateness_seconds: int = Field(ge=0)
    future_time_reference: FutureTimeReference = FutureTimeReference.GENERATED_AT
    watermark_guard: WatermarkGuardConfig
    arrival_time_skew: ArrivalTimeSkewConfig
    max_logged_rejections: int = Field(ge=0, default=100)

    @field_validator("bronze_table", "clean_table", "quarantine_table", "duplicates_table")
    @classmethod
    def _check_table(cls, value: str) -> str:
        if not TABLE_NAME_PATTERN.fullmatch(value):
            raise ValueError(f"invalid table name {value!r}; must match {TABLE_NAME_PATTERN.pattern}")
        return value

    @model_validator(mode="after")
    def _distinct_outputs(self) -> SilverConfig:
        outputs = [self.clean_table, self.quarantine_table, self.duplicates_table]
        if len(set(outputs)) != len(outputs):
            raise ValueError(f"Silver output tables must be distinct, got {outputs}")
        return self

    def fingerprint_fields(self) -> dict[str, object]:
        """The settings that affect Silver output (paths excluded: where the
        stores live does not change what is computed, and absolute local
        paths must not leak into manifests - BACKLOG FU-04)."""
        return {
            "bronze_table": self.bronze_table,
            "clean_table": self.clean_table,
            "quarantine_table": self.quarantine_table,
            "duplicates_table": self.duplicates_table,
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
            "future_time_reference": self.future_time_reference.value,
            "watermark_guard": self.watermark_guard.model_dump(mode="json"),
            "arrival_time_skew": self.arrival_time_skew.model_dump(mode="json"),
        }


def load_silver_config(path: str | Path | None = None) -> SilverConfig:
    """Load and validate the Silver config (default: bundled `default_silver.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_SILVER_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return SilverConfig.model_validate(data["silver"])
