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
        }


def load_silver_config(path: str | Path | None = None) -> SilverConfig:
    """Load and validate the Silver config (default: bundled `default_silver.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_SILVER_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return SilverConfig.model_validate(data["silver"])
