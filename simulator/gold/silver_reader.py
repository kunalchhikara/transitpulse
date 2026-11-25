"""Read the DE-06 Silver store for Gold - only through committed Silver run
manifests (BACKLOG DE-07 "Silver input contract"), mirroring how Silver reads
Bronze (`simulator/silver/bronze_reader.py`).

Gold never globs Silver *data* directories: a crashed Silver run can leave an
orphaned `run_id=<id>` partition on disk until that run_id is retried, and
only a `COMMITTED` manifest in `_runs/` makes a partition visible. Each data
file is located through the manifest's `tables[<role>].path` and verified
against it (sha256, line count == `records` == the matching Silver counter,
every record's `silver_run_id`) before a single row is used.

Each committed Silver run is a complete snapshot of the Bronze runs it
consumed (dedup and watermark state are scoped to the run - DE-06 README
"Output layout"), so Gold consumes exactly **one** Silver run; unioning
several would double-count every event they share.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from simulator.bronze.store import RUNS_DIR_NAME, STATUS_COMMITTED, validate_run_id
from simulator.gold.errors import GoldInputError

SILVER_LAYER = "silver"
#: Versions whose STRUCTURE Gold can check (every committed manifest in the
#: store is checked - fail closed). Aggregating a run additionally needs the
#: DE-08 event-time settings of 1.1.0 (`aggregate.EventTimeCheck`), so a
#: pre-DE-08 (1.0.0) run in the store does not block Gold, but is refused,
#: with a "re-run Silver" message, if it is the run selected.
SUPPORTED_SILVER_MANIFEST_VERSIONS = frozenset({"1.0.0", "1.1.0"})

ROLE_CLEAN = "clean"
ROLE_QUARANTINE = "quarantine"
ROLE_DUPLICATES = "duplicates"
#: Silver table role -> the Silver counter its committed record count must equal.
ROLE_COUNTERS: dict[str, str] = {
    ROLE_CLEAN: "valid_loaded",
    ROLE_QUARANTINE: "quarantined",
    ROLE_DUPLICATES: "duplicates_excluded",
}


@dataclass(frozen=True)
class SilverRun:
    """One committed Silver run, as described by its manifest."""

    manifest: dict[str, Any]

    @property
    def run_id(self) -> str:
        return self.manifest["run_id"]

    @property
    def processing_time(self) -> str:
        return self.manifest["processing_time"]

    @property
    def counters(self) -> dict[str, Any]:
        return self.manifest["counters"]

    def table(self, role: str) -> dict[str, Any]:
        return self.manifest["tables"][role]

    def replayed_bronze_runs(self) -> list[str]:
        """Bronze runs in this Silver run's lineage that were DE-05
        `--allow-replay` runs (BACKLOG FU-07: their rejected records are
        quarantined once per delivery, which inflates `quarantined`)."""
        return [run["run_id"] for run in self.manifest.get("bronze_runs_consumed", []) if run.get("replay_of")]

    def lineage(self) -> dict[str, Any]:
        """What Gold records about the Silver run it consumed (paths are
        relative to the Silver root - never local absolute paths, FU-04)."""
        return {
            "run_id": self.run_id,
            "processing_time": self.processing_time,
            "input_fingerprint": self.manifest.get("input_fingerprint"),
            "bronze_runs": [run["run_id"] for run in self.manifest.get("bronze_runs_consumed", [])],
            "replayed_bronze_runs": self.replayed_bronze_runs(),
            "reference_sha256": self.manifest.get("reference_sha256"),
            "tables": {
                role: {key: self.table(role)[key] for key in ("table", "path", "sha256", "records")}
                for role in ROLE_COUNTERS
            },
            "counters": {
                key: self.counters[key]
                for key in (
                    "extracted", "valid_loaded", "quarantined", "duplicates_excluded",
                    "in_order", "out_of_order", "late", "quality_percentage",
                )
            }
            | {
                # DE-08 (silver_manifest_version 1.1.0); absent in older runs.
                key: self.counters[key] for key in ("watermark_guarded",) if key in self.counters
            },
        }


def _check_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Structural checks on a COMMITTED Silver manifest; raise on anything
    that would make reading it ambiguous or unsafe."""
    run_id = manifest.get("run_id")
    if run_id != path.stem:
        raise GoldInputError(f"Silver manifest {path.name} names run_id {run_id!r}")
    validate_run_id(run_id)
    version = manifest.get("silver_manifest_version")
    if version not in SUPPORTED_SILVER_MANIFEST_VERSIONS:
        raise GoldInputError(f"Silver run {run_id!r}: unsupported silver_manifest_version {version!r}")
    if not (manifest.get("reconciliation") or {}).get("holds"):
        raise GoldInputError(f"Silver run {run_id!r} is COMMITTED but its reconciliation does not hold")
    tables = manifest.get("tables") or {}
    for role in ROLE_COUNTERS:
        if role not in tables:
            raise GoldInputError(f"Silver run {run_id!r}: manifest has no {role!r} table")
        rel = PurePosixPath(tables[role]["path"])
        if rel.is_absolute() or ".." in rel.parts:
            raise GoldInputError(f"Silver run {run_id!r}: unsafe table path {tables[role]['path']!r}")


def committed_silver_runs(silver_root: Path) -> list[SilverRun]:
    """Every committed Silver run in the store, ordered by
    (`processing_time`, `run_id`). Manifests that are not COMMITTED (FAILED
    or a crashed attempt) and non-Silver manifests are ignored."""
    runs_dir = silver_root / RUNS_DIR_NAME
    if not runs_dir.is_dir():
        return []
    runs = []
    for path in sorted(runs_dir.glob("*.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("layer") != SILVER_LAYER or manifest.get("status") != STATUS_COMMITTED:
            continue
        _check_manifest(path, manifest)
        runs.append(SilverRun(manifest))
    runs.sort(key=lambda run: (run.processing_time, run.run_id))
    return runs


def select_silver_run(silver_root: Path, run_id: str | None = None) -> tuple[SilverRun, str]:
    """The Silver run to aggregate and how it was selected.

    Default (`run_id is None`): the latest committed run - greatest
    `processing_time`, ties broken by `run_id` - because each Silver run is
    a complete snapshot, so the latest one is the most up-to-date full view.
    Explicit: exactly that run, which must be committed."""
    runs = committed_silver_runs(silver_root)
    if run_id is None:
        if not runs:
            raise GoldInputError(f"no committed Silver run in {silver_root.name!r}")
        return runs[-1], "latest_committed"
    validate_run_id(run_id)
    for run in runs:
        if run.run_id == run_id:
            return run, "explicit"
    raise GoldInputError(
        f"Silver run {run_id!r} is not committed in {silver_root.name!r}; "
        f"committed runs: {[run.run_id for run in runs]}"
    )


def read_silver_table(silver_root: Path, run: SilverRun, role: str) -> list[dict[str, Any]]:
    """Every record of one table of a committed Silver run, verified first:
    the file exists, its sha256 and line count match the manifest, the
    count equals the matching Silver counter, and every record carries this
    `silver_run_id`."""
    entry = run.table(role)
    path = silver_root / entry["path"]
    if not path.is_file():
        raise GoldInputError(f"committed Silver run {run.run_id!r}: {role} data file {entry['path']} is missing")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != entry["sha256"]:
        raise GoldInputError(
            f"committed Silver run {run.run_id!r}: {entry['path']} sha256 {digest} != manifest {entry['sha256']}"
        )
    lines = data.decode("utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    counter = ROLE_COUNTERS[role]
    if not len(lines) == entry["records"] == run.counters[counter]:
        raise GoldInputError(
            f"committed Silver run {run.run_id!r}: {role} has {len(lines)} lines, manifest records "
            f"{entry['records']}, counter {counter} {run.counters[counter]}"
        )
    records = [json.loads(line) for line in lines]
    for record in records:
        if record.get("silver_run_id") != run.run_id:
            raise GoldInputError(
                f"record silver_run_id {record.get('silver_run_id')!r} in {role} table of run {run.run_id!r}"
            )
    return records
