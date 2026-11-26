"""Shared helpers for the DE-12 batch Silver tests (not collected by pytest).

Two ways of producing committed Bronze input, both through the real
libraries:

* the real DE-11 loader (`simulator.incremental`) over a generated DE-04 /
  DE-01 source (`SbEnv`, built on the DE-11 test helpers), and
* hand-crafted DE-04-shaped batch directories ingested with the real DE-05
  `simulator.bronze.ingest_batch` (`ingest_rows`), for rows the generator
  never produces (stale / conflicting / invalid rows).

Expected values are derived from the source rows or the Bronze files with
plain `json` (an independent oracle), never from batch-Silver code.
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

from simulator.bronze import BronzeStore, fixed_clock, ingest_batch
from simulator.bronze.store import STATUS_COMMITTED
from simulator.silver_batch import (
    LINEAGE_COLUMNS,
    SILVER_DATASETS,
    SilverBatchConfig,
    load_silver_batch_config,
    read_current_table,
    run_silver_batch,
)
from simulator.tests._incremental_helpers import (
    FIXED_NOW,
    REPO_ROOT,
    Env,
    make_env,
    tree_bytes,
)

SAMPLE_OUTPUT = REPO_ROOT / "simulator" / "batch" / "sample" / "output"
PROCESSING_TIME = datetime(2031, 1, 1, tzinfo=UTC)
PROCESSING_CLOCK = fixed_clock(PROCESSING_TIME)
DELETE = object()

__all__ = ["FIXED_NOW", "REPO_ROOT", "tree_bytes"]


# -- rows ----------------------------------------------------------------------


def sample_rows(dataset: str) -> list[dict[str, Any]]:
    """The committed DE-04 sample rows of `dataset` (parsed)."""
    path = SAMPLE_OUTPUT / f"{dataset}.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sample_row(dataset: str, index: int = 0, **overrides: Any) -> dict[str, Any]:
    row = dict(sample_rows(dataset)[index])
    for key, value in overrides.items():
        if value is DELETE:
            row.pop(key, None)
        else:
            row[key] = value
    return row


def line(row: dict[str, Any]) -> bytes:
    return json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")


def pk(dataset: str, row: dict[str, Any]) -> tuple:
    return tuple(row[col] for col in SILVER_DATASETS[dataset].primary_key)


# -- Bronze input ---------------------------------------------------------------


def write_batch_dir(directory: Path, datasets: dict[str, list[bytes]]) -> Path:
    """A DE-04-shaped directory: `<dataset>.jsonl` (the given raw lines) +
    `manifest.json` with row_count/sha256, as Bronze expects."""
    directory.mkdir(parents=True, exist_ok=True)
    entries = {}
    for dataset, lines in datasets.items():
        data = b"".join(raw + b"\n" for raw in lines)
        (directory / f"{dataset}.jsonl").write_bytes(data)
        entries[dataset] = {
            "file": f"{dataset}.jsonl",
            "row_count": sum(1 for raw in lines if raw.strip()),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    (directory / "manifest.json").write_text(json.dumps({"datasets": entries}, sort_keys=True, indent=2) + "\n")
    return directory


def ingest_rows(
    bronze_dir: Path,
    run_id: str,
    datasets: dict[str, list[dict[str, Any] | bytes]],
    *,
    at: datetime,
    work_dir: Path,
) -> dict[str, Any]:
    """Land `datasets` (row dicts or raw line bytes) as one committed Bronze
    batch run `run_id` with ingestion_time `at`; returns its manifest."""
    lines = {ds: [r if isinstance(r, bytes) else line(r) for r in rows] for ds, rows in datasets.items()}
    directory = write_batch_dir(work_dir / "landing" / run_id, lines)
    result = ingest_batch(directory, run_id=run_id, output_dir=bronze_dir, clock=fixed_clock(at))
    return result.manifest


def t(day: int, hour: int = 0) -> datetime:
    """A Bronze ingestion time; distinct values fix the Bronze commit order."""
    return datetime(2030, 1, day, hour, tzinfo=UTC)


# -- Silver ---------------------------------------------------------------------


def sb_config(output_dir: Path, bronze_dir: Path, state_dir: Path | None = None, **overrides: Any) -> SilverBatchConfig:
    data = load_silver_batch_config().model_dump()
    data.update(output_dir=output_dir, bronze_dir=bronze_dir, incremental_state_dir=state_dir)
    data.update(overrides)
    return SilverBatchConfig.model_validate(data)


def run_sb(config: SilverBatchConfig, run_id: str, **kwargs: Any):
    kwargs.setdefault("clock", PROCESSING_CLOCK)
    return run_silver_batch(run_id=run_id, config=config, **kwargs)


def table(output_dir: Path, dataset: str, **kwargs: Any) -> list[dict[str, Any]]:
    return read_current_table(output_dir, dataset, **kwargs)


def business(row: dict[str, Any]) -> dict[str, Any]:
    """A current-state row without lineage columns."""
    return {k: v for k, v in row.items() if k not in LINEAGE_COLUMNS}


def table_files(output_dir: Path, prefix: str, dataset: str) -> list[dict[str, Any]]:
    """Every record of the committed `<prefix>_<dataset>` table (quarantine /
    stale) across all committed runs, read through the manifests."""
    store = BronzeStore(output_dir)
    out: list[dict[str, Any]] = []
    for path in sorted(store.runs_dir.glob("*.json")):
        manifest = json.loads(path.read_text())
        if manifest.get("status") != STATUS_COMMITTED:
            continue
        info = manifest["datasets"].get(dataset)
        entry = info["tables"].get(prefix) if info else None
        if entry:
            out.extend(json.loads(x) for x in (output_dir / entry["path"]).read_text().splitlines())
    return out


def manifest(output_dir: Path, run_id: str) -> dict[str, Any]:
    return json.loads((output_dir / "_runs" / f"{run_id}.json").read_text())


def bronze_record_counts(bronze_dir: Path) -> Counter:
    """Independent oracle: records per batch table in committed Bronze runs,
    counted from the data files with plain line counting."""
    counts: Counter = Counter()
    for m in BronzeStore(bronze_dir).committed_manifests():
        if m.get("mode") != "batch":
            continue
        for tbl, info in m["tables"].items():
            counts[tbl] += (bronze_dir / info["path"]).read_bytes().count(b"\n")
    return counts


# -- DE-11 environment ------------------------------------------------------------


@dataclass
class SbEnv:
    """A DE-11 loader environment plus a batch-Silver store next to it."""

    inc: Env
    output_dir: Path

    @property
    def bronze_dir(self) -> Path:
        return self.inc.bronze_dir

    @property
    def config(self) -> SilverBatchConfig:
        return sb_config(self.output_dir, self.inc.bronze_dir, self.inc.state_dir)

    def load(self, as_of: datetime, **kwargs: Any):
        result = self.inc.run(as_of, **kwargs)
        assert result.status in ("SUCCEEDED", "SUCCEEDED_WITH_WARNINGS"), result.status
        return result

    def silver(self, run_id: str, **kwargs: Any):
        return run_sb(self.config, run_id, **kwargs)

    def table(self, dataset: str, **kwargs: Any) -> list[dict[str, Any]]:
        return read_current_table(self.output_dir, dataset, **kwargs)

    def business_tables(self) -> dict[str, list[dict[str, Any]]]:
        return {ds: [business(r) for r in self.table(ds, include_deleted=True)] for ds in SILVER_DATASETS}


def make_sb_env(root: Path, *, service_days: int = 1, relative: bool = False, **overrides: Any) -> SbEnv:
    inc = make_env(root, service_days=service_days, relative=relative, **overrides)
    out = Path("silver_batch") if relative else root / "silver_batch"
    return SbEnv(inc, out)


def at(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def plus(value: datetime, **delta: float) -> datetime:
    return value + timedelta(**delta)


# -- CLI ------------------------------------------------------------------------


def run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    return subprocess.run(
        [sys.executable, "-m", "simulator.silver_batch", *args], cwd=cwd, env=env, capture_output=True, check=False
    )
