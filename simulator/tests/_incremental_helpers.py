"""Shared helpers for the DE-11 incremental loader tests (not collected by
pytest: no `test_` prefix).

Expected values are always recomputed from the source files themselves (an
independent oracle: plain `json.loads` per line, `datetime.fromisoformat`),
never hardcoded, so the tests survive DE-04 sample/catalogue changes.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from simulator.batch import (
    DATASET_CATALOGUE,
    generate_batch_datasets,
    load_batch_config,
    write_batch_datasets,
)
from simulator.batch.config import BatchConfig
from simulator.bronze import BronzeStore
from simulator.config import load_scale_config
from simulator.incremental import (
    IncrementalConfig,
    ManualClock,
    PipelineResult,
    load_incremental_config,
    run_incremental_load,
)
from simulator.reference import generate_reference_dataset

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DIR = REPO_ROOT / "simulator" / "batch" / "sample"
SAMPLE_BATCH_CONFIG = SAMPLE_DIR / "sample_batch.toml"
SAMPLE_SCALE_CONFIG = SAMPLE_DIR / "sample_scale.toml"

#: Clock "now" for every test run: later than any generated `updated_at`.
FIXED_NOW = datetime(2030, 1, 1, tzinfo=UTC)
REFERENCE_ENTITIES = ("depots", "stops", "routes", "vehicles")
REFERENCE_KEYS = {"depots": "depot_id", "stops": "stop_id", "routes": "route_id", "vehicles": "vehicle_id"}
INCREMENTAL_DATASETS = tuple(DATASET_CATALOGUE)
BRONZE_METADATA = {"ingestion_time", "source", "run_id"}


class SimulatedCrash(BaseException):
    """A process death: BaseException, so no `except Exception` handler in
    the loader or in Bronze can catch, audit or clean up after it."""


# -- sources -------------------------------------------------------------------


def sample_batch_config(service_days: int = 1) -> BatchConfig:
    data = load_batch_config(SAMPLE_BATCH_CONFIG).model_dump(mode="python")
    data["batch"]["service_days"] = service_days
    return BatchConfig.model_validate(data)


def write_batch_source(directory: Path, service_days: int = 1) -> dict[str, Any]:
    """Generate the DE-04 sample-config source (`service_days` days) into
    `directory` through DE-04's public API; returns its manifest."""
    batch_config = sample_batch_config(service_days)
    scale_config = load_scale_config(SAMPLE_SCALE_CONFIG)
    datasets = generate_batch_datasets(batch_config, scale_config)
    return write_batch_datasets(datasets, directory, batch_config=batch_config, scale_config=scale_config)


def write_reference_source(directory: Path) -> None:
    """DE-01 reference entities exactly as `python -m simulator.reference` writes them."""
    directory.mkdir(parents=True, exist_ok=True)
    data = generate_reference_dataset(load_scale_config(SAMPLE_SCALE_CONFIG)).to_dict()
    for name, rows in data.items():
        (directory / f"{name}.json").write_text(json.dumps(rows, sort_keys=True, indent=2) + "\n")


def rewrite_source_file(batch_dir: Path, dataset: str, data: bytes) -> None:
    """Replace a DE-04 source file and update its manifest entry, simulating a
    source system that itself produced these bytes."""
    manifest_path = batch_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    entry = manifest["datasets"][dataset]
    (batch_dir / entry["file"]).write_bytes(data)
    entry["row_count"] = sum(1 for line in data.split(b"\n") if line.strip())
    entry["sha256"] = hashlib.sha256(data).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")


def parse_ts(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    assert value.tzinfo is not None
    return value.astimezone(UTC)


def source_rows(batch_dir: Path, dataset: str) -> list[dict[str, Any]]:
    manifest = json.loads((batch_dir / "manifest.json").read_text())
    path = batch_dir / manifest["datasets"][dataset]["file"]
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def pk(row: dict[str, Any], dataset: str) -> tuple:
    return tuple(row[col] for col in DATASET_CATALOGUE[dataset].primary_key)


def source_watermark(row: dict[str, Any], dataset: str) -> datetime:
    return parse_ts(row[DATASET_CATALOGUE[dataset].watermark_column])


def expected_keys(batch_dir: Path, dataset: str, upto: datetime, after: datetime | None = None) -> Counter:
    return Counter(
        pk(row, dataset)
        for row in source_rows(batch_dir, dataset)
        if source_watermark(row, dataset) <= upto and (after is None or source_watermark(row, dataset) > after)
    )


def all_watermarks(batch_dir: Path) -> list[datetime]:
    return sorted({source_watermark(row, ds) for ds in INCREMENTAL_DATASETS for row in source_rows(batch_dir, ds)})


# -- Bronze --------------------------------------------------------------------


def bronze_records(bronze_dir: Path, table: str) -> list[dict[str, Any]]:
    return list(BronzeStore(bronze_dir).read_table(table))


def strip(record: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in record.items() if k not in BRONZE_METADATA and not k.startswith("_bronze_")}


def bronze_keys(bronze_dir: Path, dataset: str) -> Counter:
    return Counter(pk(strip(r), dataset) for r in bronze_records(bronze_dir, dataset))


def bronze_record_count(bronze_dir: Path) -> int:
    store = BronzeStore(bronze_dir)
    return sum(m["totals"]["records_written"] for m in store.committed_manifests())


def committed_run_ids(bronze_dir: Path) -> list[str]:
    return [m["run_id"] for m in BronzeStore(bronze_dir).committed_manifests()]


def tree_bytes(directory: Path) -> dict[str, bytes]:
    if not directory.exists():
        return {}
    return {
        p.relative_to(directory).as_posix(): p.read_bytes() for p in sorted(directory.rglob("*")) if p.is_file()
    }


# -- loader environment --------------------------------------------------------


@dataclass
class Env:
    root: Path
    config: IncrementalConfig

    @property
    def batch_dir(self) -> Path:
        return Path(self.config.batch_source_dir)

    @property
    def ref_dir(self) -> Path:
        return Path(self.config.reference_source_dir)

    @property
    def bronze_dir(self) -> Path:
        return Path(self.config.bronze_output_dir)

    @property
    def state_dir(self) -> Path:
        return Path(self.config.state_dir)

    def control(self, dataset: str) -> dict[str, Any] | None:
        path = self.state_dir / "control" / f"{dataset}.json"
        return json.loads(path.read_text()) if path.exists() else None

    def audit_records(self, pipeline_run_id: str, dataset: str) -> list[dict[str, Any]]:
        directory = self.state_dir / "audit" / pipeline_run_id / dataset
        return [json.loads(p.read_text()) for p in sorted(directory.glob("attempt-*.json"))]

    def run(self, as_of: datetime, *, clock: ManualClock | None = None, **kwargs: Any) -> PipelineResult:
        return run_incremental_load(
            as_of, config=self.config, clock=clock if clock is not None else ManualClock(FIXED_NOW), **kwargs
        )

    def with_config(self, **overrides: Any) -> Env:
        data = self.config.model_dump()
        data.update(overrides)
        return Env(self.root, IncrementalConfig.model_validate(data))


def make_config(base: Path, **overrides: Any) -> IncrementalConfig:
    data = load_incremental_config().model_dump()
    data.update(
        state_dir=base / "state",
        batch_source_dir=base / "batch",
        reference_source_dir=base / "ref",
        bronze_output_dir=base / "bronze",
    )
    data.update(overrides)
    return IncrementalConfig.model_validate(data)


def make_env(root: Path, *, service_days: int = 1, relative: bool = False, **overrides: Any) -> Env:
    """Sources under `root`; config paths absolute, or relative to `root`
    (`relative=True`, for byte-identical comparisons across roots - the
    caller must chdir into `root`)."""
    root.mkdir(parents=True, exist_ok=True)
    write_batch_source(root / "batch", service_days)
    write_reference_source(root / "ref")
    return Env(root, make_config(Path(".") if relative else root, **overrides))


def plus(value: datetime, **delta: float) -> datetime:
    return value + timedelta(**delta)


def run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    return subprocess.run(
        [sys.executable, "-m", "simulator.incremental", *args], cwd=cwd, env=env, capture_output=True, check=False
    )
