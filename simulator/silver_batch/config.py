"""DE-12 batch Silver configuration (`default_silver_batch.toml`).

Validated on load with pydantic (unknown keys rejected). Dataset contracts
(models, primary keys, watermark columns, foreign keys) are NOT configured
here: they come from DE-04's `DATASET_CATALOGUE` and DE-01's entity models
(`simulator/silver_batch/datasets.py`), so there is one source of truth.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

DEFAULT_SILVER_BATCH_CONFIG_PATH = Path(__file__).parent / "default_silver_batch.toml"

_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$")


class SilverBatchConfig(BaseModel):
    model_config = {"extra": "forbid"}

    output_dir: Path
    bronze_dir: Path
    incremental_state_dir: Path | None = None
    incremental_run_id_prefixes: list[str] = Field(default_factory=lambda: ["inc-", "full-"])
    max_snapshot_delete_fraction: float = Field(default=0.5, ge=0.0, le=1.0)
    max_logged_rejections: int = Field(default=100, ge=0)

    @field_validator("incremental_run_id_prefixes")
    @classmethod
    def _check_prefixes(cls, value: list[str]) -> list[str]:
        for prefix in value:
            if not _PREFIX_PATTERN.fullmatch(prefix):
                raise ValueError(f"invalid run-id prefix {prefix!r}; must match {_PREFIX_PATTERN.pattern}")
        return value

    def fingerprint_fields(self) -> dict[str, object]:
        """Settings that affect what is computed. Paths are excluded: where the
        stores live does not change the result, and absolute local paths must
        never reach a manifest (BACKLOG FU-04)."""
        return {
            "incremental_listing_enabled": self.incremental_state_dir is not None,
            "incremental_run_id_prefixes": list(self.incremental_run_id_prefixes),
            "max_snapshot_delete_fraction": self.max_snapshot_delete_fraction,
        }


def load_silver_batch_config(path: str | Path | None = None) -> SilverBatchConfig:
    """Load and validate the config (default: bundled `default_silver_batch.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_SILVER_BATCH_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return SilverBatchConfig.model_validate(data["silver_batch"])
