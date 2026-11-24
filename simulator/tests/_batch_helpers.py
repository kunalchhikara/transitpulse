"""Shared helpers for the DE-04 batch-dataset tests (not collected by pytest:
no `test_` prefix). Generation of the default-config dataset is cached so the
suite only pays for it once."""

from __future__ import annotations

import json
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

from simulator.batch.config import BatchConfig, load_batch_config
from simulator.batch.generator import BatchDatasets, generate_batch_datasets
from simulator.config import load_scale_config
from simulator.config.models import ScaleConfig
from simulator.reference.generator import ReferenceDataset, generate_reference_dataset

REPO_ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = REPO_ROOT / "simulator" / "batch"
SAMPLE_DIR = BATCH_DIR / "sample"
SAMPLE_BATCH_CONFIG = SAMPLE_DIR / "sample_batch.toml"
SAMPLE_SCALE_CONFIG = SAMPLE_DIR / "sample_scale.toml"
SAMPLE_OUTPUT_DIR = SAMPLE_DIR / "output"


@lru_cache(maxsize=1)
def default_reference() -> ReferenceDataset:
    return generate_reference_dataset(load_scale_config())


@lru_cache(maxsize=1)
def default_datasets() -> BatchDatasets:
    """Default batch config + default DE-01 scale. Treat as read-only."""
    return generate_batch_datasets(load_batch_config(), reference=default_reference())


def generate(
    batch_config: BatchConfig | None = None, scale_config: ScaleConfig | None = None
) -> tuple[BatchDatasets, ReferenceDataset]:
    batch_config = batch_config if batch_config is not None else load_batch_config()
    scale_config = scale_config if scale_config is not None else load_scale_config()
    reference = generate_reference_dataset(scale_config)
    return generate_batch_datasets(batch_config, reference=reference), reference


def with_overrides(config: BatchConfig, **sections: dict[str, Any]) -> BatchConfig:
    """Return a re-validated copy of `config` with per-section field overrides,
    e.g. `with_overrides(cfg, batch={"service_days": 2})`."""
    data = config.model_dump(mode="python")
    for section, values in sections.items():
        data[section].update(values)
    return BatchConfig.model_validate(data)


def rows_as_dicts(datasets: BatchDatasets, name: str) -> list[dict]:
    return [row.model_dump(mode="json") for row in datasets.rows(name)]


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def effective_depot_lookup(assignment_rows: list[dict]):
    """Independent point-in-time lookup built only from
    `vehicle_depot_assignments` dict rows: returns `depot(vehicle_id,
    event_date_iso)` = depot of the vehicle's latest row whose `event_date`
    is on or before `event_date_iso` (ISO dates sort lexicographically).
    Raises KeyError when no row is effective yet."""
    history: dict[str, list[tuple[str, str]]] = {}
    for row in assignment_rows:
        history.setdefault(row["vehicle_id"], []).append((row["event_date"], row["depot_id"]))
    for changes in history.values():
        changes.sort()

    def depot(vehicle_id: str, event_date: str) -> str:
        effective = [d for start, d in history[vehicle_id] if start <= event_date]
        if not effective:
            raise KeyError(f"no assignment effective for {vehicle_id} on {event_date}")
        return effective[-1]

    return depot


def run_cli(output_dir: Path, *extra: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [sys.executable, "-m", "simulator.batch", "--output-dir", str(output_dir), *extra],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    return result
