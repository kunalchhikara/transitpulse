"""DE-11 incremental loader configuration (`default_incremental.toml`).

Validated on load with pydantic (unknown keys rejected), so a typo in a mode
or policy name fails fast. `resolve_plan` turns the config into the ordered
list of datasets to load: the configured entries first, then (by default)
every `DATASET_CATALOGUE` dataset not listed, in catalogue order - so a new
DE-04 dataset is loaded incrementally without a config change.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator

from simulator.batch import DATASET_CATALOGUE

DEFAULT_INCREMENTAL_CONFIG_PATH = Path(__file__).parent / "default_incremental.toml"

#: Dataset names become Bronze table and local directory names, so they use
#: the same safe alphabet as DE-05 Bronze tables.
DATASET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]{0,63}$")
#: Run-id prefixes are part of deterministic Bronze run ids.
RUN_ID_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9]{1,8}$")


class LoadMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class BadSourcePolicy(str, Enum):
    STRICT = "strict"
    PRESERVE = "preserve"


class LateArrivalPolicy(str, Enum):
    FAIL = "fail"
    RECORD = "record"


class _Strict(BaseModel):
    model_config = {"extra": "forbid"}


class RetryConfig(_Strict):
    max_attempts: int = Field(ge=1, le=20)
    initial_backoff_seconds: float = Field(ge=0.0)
    backoff_multiplier: float = Field(ge=1.0)
    max_backoff_seconds: float = Field(ge=0.0)

    def backoff_for(self, failed_attempt: int) -> float:
        """Seconds to wait after failed attempt `failed_attempt` (1-based)."""
        if failed_attempt < 1:
            raise ValueError("failed_attempt is 1-based")
        delay = self.initial_backoff_seconds * self.backoff_multiplier ** (failed_attempt - 1)
        return min(self.max_backoff_seconds, delay)


class DatasetEntry(_Strict):
    name: str
    mode: LoadMode
    #: full mode only: file name (no directories) in `reference_source_dir`.
    source_file: str | None = None
    #: full mode only (incremental datasets take theirs from the catalogue).
    primary_key: list[str] | None = None

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not DATASET_NAME_PATTERN.fullmatch(value):
            raise ValueError(f"invalid dataset name {value!r}; must match {DATASET_NAME_PATTERN.pattern}")
        return value

    @model_validator(mode="after")
    def _check_mode_fields(self) -> DatasetEntry:
        if self.mode is LoadMode.FULL:
            if self.name in DATASET_CATALOGUE:
                raise ValueError(
                    f"{self.name!r} is a DE-04 catalogue dataset; full mode is for DE-01 reference entities"
                )
            if not self.source_file or Path(self.source_file).name != self.source_file:
                raise ValueError(f"full dataset {self.name!r} needs a plain source_file name (no directories)")
            if not self.primary_key:
                raise ValueError(f"full dataset {self.name!r} needs a non-empty primary_key")
        else:
            if self.name not in DATASET_CATALOGUE:
                raise ValueError(
                    f"incremental dataset {self.name!r} is not in DATASET_CATALOGUE "
                    f"(known: {sorted(DATASET_CATALOGUE)})"
                )
            if self.source_file is not None or self.primary_key is not None:
                raise ValueError(
                    f"incremental dataset {self.name!r}: source_file/primary_key come from the catalogue"
                )
        return self


class IncrementalConfig(_Strict):
    state_dir: Path
    batch_source_dir: Path
    reference_source_dir: Path
    bronze_output_dir: Path
    bronze_config: Path | None = None
    bad_source_policy: BadSourcePolicy = BadSourcePolicy.STRICT
    verify_source_manifest: bool = True
    source_manifest_file: str = Field(default="manifest.json", min_length=1)
    late_arrival_policy: LateArrivalPolicy = LateArrivalPolicy.FAIL
    incremental_run_id_prefix: str = "inc"
    full_run_id_prefix: str = "full"
    include_unlisted_catalogue_datasets: bool = True
    exclude_datasets: list[str] = Field(default_factory=list)
    retry: RetryConfig
    datasets: list[DatasetEntry] = Field(default_factory=list)

    @field_validator("incremental_run_id_prefix", "full_run_id_prefix")
    @classmethod
    def _check_prefix(cls, value: str) -> str:
        if not RUN_ID_PREFIX_PATTERN.fullmatch(value):
            raise ValueError(f"invalid run-id prefix {value!r}; must match {RUN_ID_PREFIX_PATTERN.pattern}")
        return value

    @model_validator(mode="after")
    def _check(self) -> IncrementalConfig:
        if self.incremental_run_id_prefix == self.full_run_id_prefix:
            raise ValueError("incremental_run_id_prefix and full_run_id_prefix must differ")
        names = [entry.name for entry in self.datasets]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"dataset listed more than once: {duplicates}")
        unknown = sorted(set(self.exclude_datasets) - set(DATASET_CATALOGUE) - set(names))
        if unknown:
            raise ValueError(f"exclude_datasets names unknown datasets: {unknown}")
        if Path(self.source_manifest_file).name != self.source_manifest_file:
            raise ValueError("source_manifest_file must be a plain file name")
        if not resolve_plan(self):
            raise ValueError("no datasets to load (all excluded?)")
        return self


@dataclass(frozen=True)
class DatasetPlan:
    """One dataset to load, fully resolved."""

    name: str
    mode: LoadMode
    primary_key: tuple[str, ...]
    #: incremental: the catalogue's watermark column; full: None.
    watermark_column: str | None
    #: incremental: the catalogue's partition column; full: None.
    partition_column: str | None
    #: full: file name in `reference_source_dir`; incremental: None (the
    #: file comes from the DE-04 source manifest).
    source_file: str | None

    @property
    def is_full(self) -> bool:
        return self.mode is LoadMode.FULL


def _plan_for(entry: DatasetEntry) -> DatasetPlan:
    if entry.mode is LoadMode.FULL:
        return DatasetPlan(entry.name, LoadMode.FULL, tuple(entry.primary_key or ()), None, None, entry.source_file)
    return _catalogue_plan(entry.name)


def _catalogue_plan(name: str) -> DatasetPlan:
    spec = DATASET_CATALOGUE[name]
    return DatasetPlan(
        name=name,
        mode=LoadMode.INCREMENTAL,
        primary_key=tuple(spec.primary_key),
        watermark_column=spec.watermark_column,
        partition_column=spec.partition_column,
        source_file=None,
    )


def resolve_plan(config: IncrementalConfig) -> list[DatasetPlan]:
    """Ordered datasets to load: configured entries (minus exclusions), then
    unlisted catalogue datasets in catalogue order (if enabled)."""
    excluded = set(config.exclude_datasets)
    plan = [_plan_for(entry) for entry in config.datasets if entry.name not in excluded]
    if config.include_unlisted_catalogue_datasets:
        listed = {entry.name for entry in config.datasets}
        plan.extend(
            _catalogue_plan(name) for name in DATASET_CATALOGUE if name not in listed and name not in excluded
        )
    return plan


def load_incremental_config(path: str | Path | None = None) -> IncrementalConfig:
    """Load and validate the loader config (default: bundled `default_incremental.toml`)."""
    resolved = Path(path) if path is not None else DEFAULT_INCREMENTAL_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return IncrementalConfig.model_validate(data["incremental"])
