"""DE-07 Gold configuration (`default_gold.toml`).

A separate config surface from DE-01..DE-06 (imported, never modified).
Validated on load with pydantic (unknown keys rejected), so a typo fails fast
instead of silently falling back to a default.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from simulator.bronze.config import TABLE_NAME_PATTERN

DEFAULT_GOLD_CONFIG_PATH = Path(__file__).parent / "default_gold.toml"
MINUTES_PER_DAY = 24 * 60


class GoldConfig(BaseModel):
    model_config = {"extra": "forbid"}

    output_dir: Path
    silver_dir: Path
    route_performance_table: str
    period_minutes: int = Field(gt=0, le=MINUTES_PER_DAY)
    utc_offset_hours: int = Field(ge=-12, le=14)
    write_csv: bool = True

    @field_validator("route_performance_table")
    @classmethod
    def _check_table(cls, value: str) -> str:
        if not TABLE_NAME_PATTERN.fullmatch(value):
            raise ValueError(f"invalid table name {value!r}; must match {TABLE_NAME_PATTERN.pattern}")
        return value

    @field_validator("period_minutes")
    @classmethod
    def _divides_a_day(cls, value: int) -> int:
        # Buckets are aligned to local midnight; a period that does not divide
        # a day would make bucket boundaries drift from one day to the next.
        if MINUTES_PER_DAY % value:
            raise ValueError(f"period_minutes must divide {MINUTES_PER_DAY} (a day), got {value}")
        return value

    def fingerprint_fields(self) -> dict[str, object]:
        """Settings that affect Gold output (store paths excluded: where the
        stores live does not change what is computed, and absolute local
        paths must not leak into manifests - BACKLOG FU-04)."""
        return {
            "route_performance_table": self.route_performance_table,
            "period_minutes": self.period_minutes,
            "utc_offset_hours": self.utc_offset_hours,
            "write_csv": self.write_csv,
        }


def load_gold_config(path: str | Path | None = None) -> GoldConfig:
    """Load and validate the Gold config (default: bundled `default_gold.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_GOLD_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return GoldConfig.model_validate(data["gold"])
