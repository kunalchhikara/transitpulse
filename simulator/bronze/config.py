"""DE-05 Bronze ingestion configuration (`default_bronze.toml`).

A separate config surface from DE-01..DE-04 (which this story imports but
never modifies). Validated on load with pydantic (unknown keys rejected), so
a typo in a policy name fails fast instead of silently falling back.
"""

from __future__ import annotations

import re
import tomllib
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

DEFAULT_BRONZE_CONFIG_PATH = Path(__file__).parent / "default_bronze.toml"

#: Table names become directory names in the local store, so they are
#: restricted to a safe identifier alphabet (no separators, no `..`). A
#: leading underscore is reserved for store-internal directories (`_runs`,
#: `_staging`), so a table can never shadow them or escape orphan cleanup.
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]{0,63}$")


class ParseErrorPolicy(str, Enum):
    """How a line that cannot be stored as-is is handled. See README DE-05."""

    PRESERVE = "preserve"
    FAIL = "fail"


class StreamSourceConfig(BaseModel):
    model_config = {"extra": "forbid"}

    source: str = Field(min_length=1)
    table: str

    @field_validator("table")
    @classmethod
    def _check_table(cls, value: str) -> str:
        if not TABLE_NAME_PATTERN.fullmatch(value):
            raise ValueError(f"invalid table name {value!r}; must match {TABLE_NAME_PATTERN.pattern}")
        return value


class BatchSourceConfig(BaseModel):
    model_config = {"extra": "forbid"}

    source: str = Field(min_length=1)
    record_source_template: str = Field(min_length=1)
    source_manifest_file: str = Field(min_length=1)

    @field_validator("record_source_template")
    @classmethod
    def _check_template(cls, value: str) -> str:
        if "{dataset}" not in value:
            raise ValueError("record_source_template must contain '{dataset}'")
        value.format(dataset="x")  # raises on any other placeholder
        return value

    def record_source(self, dataset: str) -> str:
        return self.record_source_template.format(dataset=dataset)


class BronzeConfig(BaseModel):
    model_config = {"extra": "forbid"}

    output_dir: Path
    batch_input_dir: Path
    parse_error_policy: ParseErrorPolicy = ParseErrorPolicy.PRESERVE
    verify_source_manifest: bool = True
    #: Cap on per-record parse-error WARNING log lines per run (every error is
    #: still counted in the manifest and preserved in Bronze).
    max_logged_parse_errors: int = Field(ge=0, default=100)
    stream: StreamSourceConfig
    batch: BatchSourceConfig


def load_bronze_config(path: str | Path | None = None) -> BronzeConfig:
    """Load and validate the Bronze config (default: bundled `default_bronze.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_BRONZE_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return BronzeConfig.model_validate(data["bronze"])
